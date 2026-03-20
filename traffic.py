"""
traffic.cv Bulk Scraper — Multi-Worker (v3)
- Each worker has its own Chrome window
- No shared driver → no data mixing between workers
- If CAPTCHA appears, press ENTER to continue
- Output: output_1.csv (saved live after each domain)
"""

import csv
import re
import time
import threading
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# ── Config ─────────────────────────────────────────────────
INPUT_FILE  = "input.csv"
OUTPUT_FILE = "output_1.csv"
NUM_WORKERS = 5    # number of Chrome windows (3 = safe, 5 = fast, 10 = risky)
LOAD_WAIT   = 3    # seconds to wait for JS to render

FIELDNAMES = [
    "url", "total_visits", "visits_change",
    "month_1", "visits_month_1",
    "month_2", "visits_month_2",
    "month_3", "visits_month_3",
    "scraped_at", "status"
]

write_lock   = threading.Lock()
print_lock   = threading.Lock()
captcha_lock = threading.Lock()
counter_lock = threading.Lock()

ok_count  = 0
err_count = 0

# ── CSV Helpers ────────────────────────────────────────────
def init_csv():
    """Create output file and write header row."""
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=FIELDNAMES).writeheader()

def save_row(row):
    """Append a single result row to the output CSV (thread-safe)."""
    with write_lock:
        with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=FIELDNAMES).writerow(row)

# ── Chrome Setup ───────────────────────────────────────────
def make_driver():
    """Launch a Chrome instance with anti-detection settings."""
    opts = Options()
    opts.add_argument("--start-maximized")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--disable-blink-features=AutomationControlled")
    return webdriver.Chrome(options=opts)

# ── HTML Parser ────────────────────────────────────────────
def parse_html(html, domain):
    """Extract traffic stats from the rendered page HTML."""
    soup  = BeautifulSoup(html, "html.parser")
    lines = [l.strip() for l in soup.get_text("\n").splitlines() if l.strip()]

    row = {f: "" for f in FIELDNAMES}
    row["url"]        = domain
    row["scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row["status"]     = "ok"

    # Extract total visits and % change
    for i, line in enumerate(lines):
        if "Total Visits" in line:
            for j in range(i+1, min(i+8, len(lines))):
                if re.match(r"[\d\.]+[MKB]?$", lines[j]):
                    row["total_visits"] = lines[j]
                    chunk = " ".join(lines[i:i+8])
                    m = re.search(r"([+-]?\d+\.\d+%)", chunk)
                    if m:
                        row["visits_change"] = m.group(1)
                    break
            break

    # Extract month labels from chart X-axis
    x_labels = []
    for el in soup.find_all("text", class_=re.compile("recharts-cartesian-axis-tick-value")):
        ts = el.find("tspan")
        if ts and re.match(r"\d{4}/\d{2}", ts.text.strip()):
            x_labels.append(ts.text.strip())

    def y_to_visits(y):
        """Convert chart Y pixel coordinate to visit count."""
        return f"{round((260 - float(y)) / 260 * 4_000_000 / 1000) * 1000:,.0f}"

    # Extract visit values for each month from chart path coordinates
    path_el = soup.find("path", class_=re.compile("recharts-area-curve"))
    if path_el:
        coords = re.findall(r"([0-9.]+),([0-9.]+)", path_el.get("d", ""))
        for idx, kx in enumerate([70, 307.5, 545]):
            if idx < len(x_labels) and coords:
                closest = min(coords, key=lambda c: abs(float(c[0]) - kx))
                row[f"month_{idx+1}"]        = x_labels[idx]
                row[f"visits_month_{idx+1}"] = y_to_visits(closest[1])
    return row

# ── Block / CAPTCHA Detection ──────────────────────────────
def is_blocked(html):
    """Return True if the page is a Cloudflare block or empty page."""
    if "Just a moment" in html or "Enable JavaScript" in html:
        return True
    if "Total Visits" not in html and len(html) < 5000:
        return True
    return False

# ── Thread-safe Print ──────────────────────────────────────
def safe_print(msg):
    """Print without output getting mixed between threads."""
    with print_lock:
        print(msg, flush=True)

# ── CAPTCHA Handler ────────────────────────────────────────
def handle_captcha(driver, domain):
    """
    Pause all workers, let user solve CAPTCHA in browser,
    then reload the page and return the new HTML.
    """
    with captcha_lock:
        safe_print(f"\n\n  CAPTCHA detected on: {domain}")
        safe_print(f"  >>> Solve it in the browser, then press ENTER to continue: ")
        input()
        driver.get(f"https://traffic.cv/{domain}")
        time.sleep(LOAD_WAIT + 1)
        return driver.page_source

# ── Worker Thread ──────────────────────────────────────────
def worker(worker_id, domain_queue, total):
    """
    Each worker runs in its own thread with its own Chrome window.
    Pulls domains from the shared queue until it's empty.
    """
    global ok_count, err_count

    driver = make_driver()
    safe_print(f"  [Worker {worker_id}] Chrome started")

    while True:
        # Get next domain from queue
        try:
            idx, domain = domain_queue.get_nowait()
        except Exception:
            break  # Queue is empty, worker is done

        try:
            driver.get(f"https://traffic.cv/{domain}")
            time.sleep(LOAD_WAIT)
            html = driver.page_source

            # Handle CAPTCHA if detected
            if is_blocked(html):
                html = handle_captcha(driver, domain)

            row = parse_html(html, domain)

            with counter_lock:
                if row["total_visits"]:
                    ok_count += 1
                    status_str = f"OK  {row['total_visits']} ({row['visits_change']})"
                else:
                    row["status"] = "no_data"
                    err_count += 1
                    status_str = "WARNING  No data found"

        except Exception as e:
            row = {f: "" for f in FIELDNAMES}
            row.update({
                "url": domain,
                "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "status": f"ERROR: {e}"
            })
            with counter_lock:
                err_count += 1
            status_str = f"ERROR  {e}"

        safe_print(f"  ({idx}/{total}) [W{worker_id}] {domain} ... {status_str}")
        save_row(row)
        domain_queue.task_done()

    driver.quit()
    safe_print(f"  [Worker {worker_id}] Finished, Chrome closed")

# ── Main ───────────────────────────────────────────────────
def main():
    import queue

    # Load domains from input CSV
    domains = []
    with open(INPUT_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            d = row.get("url", "").strip()
            if d:
                domains.append(d)

    total   = len(domains)
    workers = min(NUM_WORKERS, total)

    print("=" * 55)
    print(f"  traffic.cv Multi-Worker Scraper — {total} domains")
    print("=" * 55)
    print(f"  Input   : {INPUT_FILE}")
    print(f"  Output  : {OUTPUT_FILE}")
    print(f"  Workers : {workers} Chrome windows")
    print(f"  ETA     : ~{round(total * LOAD_WAIT / workers / 60, 1)} min")
    print("=" * 55)

    init_csv()

    # Fill the shared queue with all domains
    domain_queue = queue.Queue()
    for idx, domain in enumerate(domains, 1):
        domain_queue.put((idx, domain))

    # Launch worker threads — each gets its own Chrome window
    threads = []
    for wid in range(1, workers + 1):
        t = threading.Thread(
            target=worker,
            args=(wid, domain_queue, total),
            daemon=True
        )
        threads.append(t)
        t.start()
        time.sleep(1.5)  # stagger Chrome launches to avoid startup conflicts

    # Wait for all workers to finish
    for t in threads:
        t.join()

    print(f"\n{'='*55}")
    print(f"  Done! Success: {ok_count} | Errors: {err_count}")
    print(f"  Output saved to: {OUTPUT_FILE}")
    print(f"{'='*55}")

if __name__ == "__main__":
    main()
