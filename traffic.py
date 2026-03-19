"""
traffic.cv Bulk Scraper — Selenium Direct
- Opens one Chrome window and scrapes each domain one by one
- If CAPTCHA appears, press ENTER in terminal to continue
- 1 second gap between each request
- Output: output.csv (saved live after every row)
"""

import csv
import re
import time
import threading
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# ── Configuration ──────────────────────────────────────────
INPUT_FILE  = "input.csv"   # Input CSV file with 'url' column
OUTPUT_FILE = "output.csv"  # Output CSV file where results are saved
GAP         = 1             # Seconds to wait between each request

# Output CSV column names
FIELDNAMES = [
    "url", "total_visits", "visits_change",
    "month_1", "visits_month_1",
    "month_2", "visits_month_2",
    "month_3", "visits_month_3",
    "scraped_at", "status"
]

write_lock = threading.Lock()  # Prevents multiple threads writing at same time

# ── CSV Functions ──────────────────────────────────────────
def init_csv():
    """Create output CSV file and write header row."""
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=FIELDNAMES).writeheader()

def save_row(row):
    """Append a single row to the output CSV file (thread-safe)."""
    with write_lock:
        with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=FIELDNAMES).writerow(row)

# ── Chrome Browser Setup ───────────────────────────────────
def make_driver():
    """Create and return a Chrome browser instance with anti-detection settings."""
    opts = Options()
    opts.add_argument("--start-maximized")
    # Hide automation flags so website doesn't detect bot
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--disable-blink-features=AutomationControlled")
    return webdriver.Chrome(options=opts)

# ── HTML Data Parser ───────────────────────────────────────
def parse_html(html, domain):
    """
    Extract traffic data from the page HTML.
    Returns a dict with total visits, change %, and monthly breakdown.
    """
    soup  = BeautifulSoup(html, "html.parser")
    lines = [l.strip() for l in soup.get_text("\n").splitlines() if l.strip()]

    # Start with empty row
    row = {f: "" for f in FIELDNAMES}
    row["url"]        = domain
    row["scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row["status"]     = "ok"

    # Find "Total Visits" section and extract the number + change %
    for i, line in enumerate(lines):
        if "Total Visits" in line:
            for j in range(i+1, min(i+8, len(lines))):
                if re.match(r"[\d\.]+[MKB]?$", lines[j]):
                    row["total_visits"] = lines[j]
                    # Look for percentage change like -32.11% or +5.2%
                    chunk = " ".join(lines[i:i+8])
                    m = re.search(r"([+-]?\d+\.\d+%)", chunk)
                    if m:
                        row["visits_change"] = m.group(1)
                    break
            break

    # Extract month labels from the chart X-axis (e.g. 2025/12, 2026/01)
    x_labels = []
    for el in soup.find_all("text", class_=re.compile("recharts-cartesian-axis-tick-value")):
        ts = el.find("tspan")
        if ts and re.match(r"\d{4}/\d{2}", ts.text.strip()):
            x_labels.append(ts.text.strip())

    def y_to_visits(y):
        """Convert SVG Y coordinate to estimated visit count."""
        return f"{round((260 - float(y)) / 260 * 4_000_000 / 1000) * 1000:,.0f}"

    # Extract visit estimates from the SVG area chart path
    path_el = soup.find("path", class_=re.compile("recharts-area-curve"))
    if path_el:
        coords = re.findall(r"([0-9.]+),([0-9.]+)", path_el.get("d", ""))
        # X positions 70, 307.5, 545 correspond to month 1, 2, 3 on the chart
        for idx, kx in enumerate([70, 307.5, 545]):
            if idx < len(x_labels) and coords:
                closest = min(coords, key=lambda c: abs(float(c[0]) - kx))
                row[f"month_{idx+1}"]        = x_labels[idx]
                row[f"visits_month_{idx+1}"] = y_to_visits(closest[1])
    return row

# ── Block / CAPTCHA Detection ──────────────────────────────
def is_blocked(html):
    """
    Check if the page is showing a Cloudflare challenge or is empty.
    Returns True if blocked, False if page loaded normally.
    """
    if "Just a moment" in html or "Enable JavaScript" in html:
        return True
    # If page is too small and has no traffic data, likely blocked
    if "Total Visits" not in html and len(html) < 5000:
        return True
    return False

# ── Main Scraper ───────────────────────────────────────────
def main():
    # Read all domains from input CSV
    domains = []
    with open(INPUT_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            d = row.get("url", "").strip()
            if d:
                domains.append(d)

    total = len(domains)
    print("=" * 50)
    print(f"  traffic.cv Scraper — {total} domains")
    print("=" * 50)
    print(f"  Input  : {INPUT_FILE}")
    print(f"  Output : {OUTPUT_FILE}")
    print(f"  Gap    : {GAP}s per request")
    print("=" * 50)

    init_csv()

    # Open Chrome browser once — reuse for all domains
    driver = make_driver()

    ok_count  = 0
    err_count = 0

    for idx, domain in enumerate(domains, 1):
        url = f"https://traffic.cv/{domain}"
        print(f"  ({idx}/{total}) {domain} ...", end=" ", flush=True)

        try:
            driver.get(url)
            time.sleep(2)  # Wait for JavaScript to fully render the page

            html = driver.page_source

            # If blocked or CAPTCHA detected, ask user to solve it
            if is_blocked(html):
                print(f"\n\n  🔒 CAPTCHA / Block detected!")
                print(f"  >>> Solve it in browser, then press ENTER: ", end="")
                input()
                # Retry the same domain after CAPTCHA is solved
                driver.get(url)
                time.sleep(2)
                html = driver.page_source

            # Parse traffic data from page
            row = parse_html(html, domain)

            if row["total_visits"]:
                print(f"✅ {row['total_visits']} ({row['visits_change']})")
                ok_count += 1
            else:
                row["status"] = "no_data"
                print(f"⚠️  No data found")
                err_count += 1

        except Exception as e:
            # Save error row so we know which domains failed
            row = {f: "" for f in FIELDNAMES}
            row.update({
                "url": domain,
                "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "status": f"ERROR: {e}"
            })
            print(f"❌ {e}")
            err_count += 1

        # Save row immediately to CSV (crash-safe)
        save_row(row)

        # Print progress every 100 domains
        if idx % 100 == 0:
            print(f"\n  ── {idx}/{total} done | ✅ {ok_count} | ❌ {err_count} ──\n")

        # Wait before next request to avoid being rate-limited
        time.sleep(GAP)

    driver.quit()

    print(f"\n{'='*50}")
    print(f"  ✅ Done! Success: {ok_count} | Errors: {err_count}")
    print(f"  📁 Output saved to: {OUTPUT_FILE}")
    print(f"{'='*50}")

if __name__ == "__main__":
    main()
