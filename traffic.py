import csv
import re
import time
import threading
import queue
import os
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- CONFIG ---
INPUT_FILE  = "input.csv"
OUTPUT_FILE = "dang+2.csv"
FAILED_FILE = "failed_retries.csv"
NUM_WORKERS = 15  # Slightly lowered for better stability on system resources
BATCH_SIZE  = 10
LOAD_TIMEOUT = 45

FIELDNAMES = [
    "url", "total_visits", "visits_change",
    "avg_duration", "pages_per_visit", "bounce_rate",
    "registration", "expiration",
    "month_1", "visits_month_1",
    "month_2", "visits_month_2",
    "month_3", "visits_month_3",
    "scraped_at", "status"
]

write_lock   = threading.Lock()
print_lock   = threading.Lock()
failed_lock  = threading.Lock()
counter_lock = threading.Lock()
ok_count = err_count = 0

def init_files(file_path):
    if not os.path.exists(file_path):
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            headers = FIELDNAMES if "output" in file_path else ["url"]
            csv.DictWriter(f, fieldnames=headers).writeheader()

def save_rows(file_path, rows, fields):
    with write_lock:
        with open(file_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writerows(rows)

def make_driver():
    opts = Options()
    opts.add_argument("--start-maximized")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    # Avoid connection closed errors by setting a page load strategy
    opts.page_load_strategy = 'normal' 
    return webdriver.Chrome(options=opts)

def safe_print(msg):
    with print_lock:
        print(msg, flush=True)

def worker(worker_id, batch_queue, total_batches):
    global ok_count, err_count
    driver = make_driver()
    
    while True:
        try:
            batch_data = batch_queue.get_nowait()
            batch_idx, batch = batch_data
        except queue.Empty: break

        domains_str = ",".join(batch)
        url = f"https://traffic.cv/bulk?domains={domains_str}"

        try:
            # --- CRASH RECOVERY ---
            try:
                driver.get(url)
            except Exception:
                safe_print(f"  [W{worker_id}] Chrome Instance Error. Restarting browser...")
                try: driver.quit()
                except: pass
                driver = make_driver()
                driver.get(url)

            # --- ROBUST WAIT LOGIC ---
            start_t = time.time()
            while time.time() - start_t < LOAD_TIMEOUT:
                current_html = driver.page_source
                temp_soup = BeautifulSoup(current_html, "html.parser")
                found_h2s = len(temp_soup.find_all("h2"))
                skeletons = len(temp_soup.select("[data-slot='skeleton'], .animate-pulse"))
                
                if found_h2s >= len(batch) and skeletons == 0:
                    break
                time.sleep(2)

            time.sleep(2) # Final buffer for chart JS
            parsed = parse_bulk_page(driver.page_source, batch)
            
            success_rows = []
            failed_to_log = []

            for domain in batch:
                data = parsed.get(domain)
                # Unregistered domains are a final answer, not a failure: record
                # them in the output and keep them out of the retry/failed list.
                if data and data.get("status") == "unregistered":
                    success_rows.append(data)
                    with counter_lock: ok_count += 1
                elif data and data["total_visits"]:
                    success_rows.append(data)
                    with counter_lock: ok_count += 1
                else:
                    failed_to_log.append({"url": domain})
                    with counter_lock: err_count += 1
            
            if success_rows: save_rows(OUTPUT_FILE, success_rows, FIELDNAMES)
            if failed_to_log: save_rows(FAILED_FILE, failed_to_log, ["url"])
            
            safe_print(f"  (Batch {batch_idx}/{total_batches}) [W{worker_id}] {len(batch)} processed.")

        except Exception as e:
            safe_print(f"  [W{worker_id}] Fatal Batch Error: {str(e)[:50]}")
            save_rows(FAILED_FILE, [{"url": d} for d in batch], ["url"])

        batch_queue.task_done()
    
    driver.quit()

def parse_bulk_page(html, domains):
    soup = BeautifulSoup(html, "html.parser")
    results = {}
    for h2 in soup.find_all("h2"):
        name = h2.get_text(strip=True)
        card = h2
        for _ in range(6):
            card = card.parent
            if card and "space-y" in " ".join(card.get("class", [])): break
        
        for d in domains:
            if d.lower() in name.lower() or name.lower() in d.lower():
                results[d] = parse_card_details(card, d)
                break
    return results

def parse_card_details(card_soup, domain):
    row = {f: "" for f in FIELDNAMES}
    row.update({"url": domain, "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "status": "ok"})

    # Detect domains the site reports as not registered. These will never have
    # traffic data, so we mark them instead of treating them as scrape failures.
    card_text = card_soup.get_text(" ", strip=True).lower()
    if "unregistered domain" in card_text or "not registered" in card_text:
        row["status"] = "unregistered"
        return row

    stat_blocks = card_soup.find_all("div", class_=re.compile("rounded-md.*bg-muted"))
    for block in stat_blocks:
        label_el = block.find("p", class_=re.compile("text-muted-foreground"))
        value_el = block.find("div", class_=re.compile("font-semibold"))
        if label_el and value_el:
            lbl, val = label_el.get_text(strip=True), value_el.get_text(strip=True)
            if "Total Visits" in lbl:
                m = re.match(r"([\d\.]+[KMB]?)(.*)", val)
                if m: row["total_visits"], row["visits_change"] = m.group(1), m.group(2).strip()
            elif "Avg. Duration" in lbl: row["avg_duration"] = val
            elif "Pages per Visit" in lbl: row["pages_per_visit"] = val
            elif "Bounce Rate" in lbl: row["bounce_rate"] = val
            elif "Registration" in lbl: row["registration"] = val
            elif "Expiration" in lbl: row["expiration"] = val
    return row

def run_scraper(input_csv):
    domains = []
    if not os.path.exists(input_csv): return
    with open(input_csv, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = [r for r in reader if r and r[0].strip()]
        if rows and rows[0][0].strip().lower() == "url":
            rows = rows[1:]
        domains = [r[0].strip() for r in rows if r[0].strip()]
    
    if not domains: return

    batches = [domains[i:i+BATCH_SIZE] for i in range(0, len(domains), BATCH_SIZE)]
    q = queue.Queue()
    for idx, b in enumerate(batches, 1): q.put((idx, b))

    threads = []
    num_threads = min(NUM_WORKERS, len(batches))
    for i in range(1, num_threads + 1):
        t = threading.Thread(target=worker, args=(i, q, len(batches)), daemon=True)
        t.start()
        threads.append(t)
        time.sleep(1.5)
    
    for t in threads: t.join()

if __name__ == "__main__":
    init_files(OUTPUT_FILE)
    if os.path.exists(FAILED_FILE): os.remove(FAILED_FILE)
    init_files(FAILED_FILE)

    print(f"--- Starting Main Pass ---")
    run_scraper(INPUT_FILE)

    # --- AUTO RETRY PASS ---
    if os.path.exists(FAILED_FILE):
        with open(FAILED_FILE, "r") as f:
            failed_count = sum(1 for line in f) - 1
        
        if failed_count > 0:
            print(f"\n--- Starting Retry Pass ({failed_count} domains to retry) ---")
            os.rename(FAILED_FILE, "temp_retry.csv")
            init_files(FAILED_FILE) # Re-init for Pass 2 failures
            run_scraper("temp_retry.csv")
            if os.path.exists("temp_retry.csv"): os.remove("temp_retry.csv")

    print(f"\nScraping Complete. Successful results saved to {OUTPUT_FILE}")
    if os.path.exists(FAILED_FILE) and os.path.getsize(FAILED_FILE) > 10:
        print(f"Final missed domains are logged in {FAILED_FILE}")
