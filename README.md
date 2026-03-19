# Traffic Bulk Scraper

Scrapes traffic data for thousands of domains from traffic.cv and saves results to a CSV file.

---

## Requirements

- Python 3.8+
- Google Chrome browser installed
- ChromeDriver (auto-managed)

### Install dependencies

```bash
pip install selenium beautifulsoup4
```

---

## Setup

1. Place your input file as `input.csv` in the same folder as `traffic.py`
2. Make sure `input.csv` has a column named `url`

### Example `input.csv`

```
url
primalqueen.com
succulentsbox.com
adoredbeast.com
```

---

## How to Run

```bash
cd /path/to/your/folder
python3 traffic.py
```

---

## How It Works

1. Script reads all domains from `input.csv`
2. Opens a real Chrome browser window
3. Visits each domain on traffic.cv one by one
4. If a CAPTCHA or block appears:
   - Terminal will prompt you
   - Solve the CAPTCHA in the browser
   - Press **ENTER** in terminal to continue
5. Data is saved to `output.csv` after every single row (crash-safe)

---

## Output File — `output.csv`

| Column | Description |
|---|---|
| `url` | Domain name |
| `total_visits` | Total visits (e.g. 2.67M) |
| `visits_change` | Change vs previous period (e.g. -32.11%) |
| `month_1` | Most recent month label (e.g. 2026/02) |
| `visits_month_1` | Estimated visits for month 1 |
| `month_2` | Previous month label |
| `visits_month_2` | Estimated visits for month 2 |
| `month_3` | Oldest month label |
| `visits_month_3` | Estimated visits for month 3 |
| `scraped_at` | Timestamp when row was scraped |
| `status` | `ok`, `no_data`, or `ERROR: ...` |

---

## Configuration

Edit these values at the top of `traffic.py`:

| Variable | Default | Description |
|---|---|---|
| `INPUT_FILE` | `input.csv` | Input file name |
| `OUTPUT_FILE` | `output.csv` | Output file name |
| `GAP` | `1` | Seconds between each request |

---

## Notes

- Script uses a real Chrome browser so Cloudflare does not block it
- If you get blocked frequently, increase `GAP` to `2` or `3`
- Output CSV is written live — if script crashes, already scraped rows are safe
- Progress is printed every 100 domains

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `FileNotFoundError: input.csv` | Run script from same folder as input.csv — use `cd` first |
| Chrome doesn't open | Make sure Google Chrome is installed |
| All rows show `no_data` | Page didn't load — increase `time.sleep(2)` to `time.sleep(4)` |
| Stuck on CAPTCHA loop | Solve CAPTCHA fully in browser, wait for page to load, then press ENTER |
