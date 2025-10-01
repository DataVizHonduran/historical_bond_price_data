# ETF Holdings Price Tracker

Track historical holdings data for Emerging Markets Bond ETFs using SQLite.

## 📁 Project Structure

```
etf-price-tracker/
├── tracker.py          # Main script - fetches and saves data
├── query_helper.py     # Helper functions for analyzing data
├── data/
│   └── etf_holdings.db # SQLite database (created automatically)
└── README.md
```

## 🚀 Quick Start

### 1. Run the tracker daily

```bash
python tracker.py
```

This will:
- Fetch latest holdings for all configured ETFs
- Save to SQLite database
- Skip if today's data already exists

### 2. Query your data

```bash
python query_helper.py
```

Or use in your own scripts:

```python
from query_helper import *

# Get latest holdings
df = get_latest_holdings('EMBI')

# Track a bond over time
history = track_bond_over_time(name='MEXICO', etf_code='EMBI')

# Compare two dates
changes = compare_dates('EMBI', '2025-09-01', '2025-10-01')
```

## ➕ Adding New ETFs

Open `tracker.py` and add to the `ETF_CONFIG` dictionary:

```python
ETF_CONFIG = {
    'YOUR_ETF': {
        'name': 'Full ETF Name',
        'url': 'https://www.ishares.com/...',
        'header_row': 9  # Row where column names start
    },
    # ... existing ETFs
}
```

That's it! The tracker will automatically include it next run.

## 📊 Current ETFs

- **EMBI** - iShares J.P. Morgan USD Emerging Markets Bond ETF
- **CEMBI** - iShares Emerging Markets Corporate Bond ETF
- **GBI** - iShares Emerging Markets Local Currency Bond ETF
- **EMHY** - iShares Emerging Markets High Yield Bond ETF

## 🔍 Useful Queries

### Show all available data
```python
get_database_stats()
get_available_dates()
```

### Get top holdings
```python
get_top_holdings('EMBI', top_n=10)
```

### Track a specific bond
```python
track_bond_over_time(name='BRAZIL', etf_code='EMBI')
```

### Country exposure over time
```python
get_country_exposure_over_time('EMBI', 'MEXICO')
```

### Compare dates
```python
changes = compare_dates('EMBI', '2025-09-01', '2025-10-01')
print(changes.head(20))  # Top 20 changes
```

## 💡 Tips

1. **Run daily** - Set up a cron job or Windows Task Scheduler to run automatically
2. **Check for duplicates** - The script prevents duplicate entries for the same day
3. **Backup** - The database is just one file (`data/etf_holdings.db`) - copy it to backup
4. **Export to CSV** - Any query result can be exported: `df.to_csv('output.csv')`

## 🛠️ Requirements

```bash
pip install pandas sqlite3
```

(sqlite3 is built into Python, so you probably only need pandas)

## 📅 Scheduling (Optional)

### macOS/Linux (cron)
```bash
crontab -e
# Add this line to run daily at 6 PM:
0 18 * * * cd /path/to/etf-price-tracker && python tracker.py
```

### Windows (Task Scheduler)
Create a task that runs `python tracker.py` daily

## 🐛 Troubleshooting

**"Database is locked"** - Only run one instance at a time

**"No data for ETF"** - Check the URL is still valid in `ETF_CONFIG`

**"Duplicate entry"** - Already ran today, database prevented duplicate

## 📈 Future Ideas

- Add email notifications when yields change significantly
- Create automated reports
- Build a web dashboard
- Add more ETF providers (Vanguard, etc.)
- Track price changes vs yield changes
