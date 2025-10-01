"""
ETF Holdings Price Tracker
Fetches ETF holdings data and stores it in SQLite database
"""

import pandas as pd
import sqlite3
from datetime import datetime
import os

# ============================================================================
# CONFIGURATION - Add new ETFs here!
# ============================================================================

ETF_CONFIG = {
    'CEMBI': {
        'name': 'iShares Emerging Markets Corporate Bond ETF',
        'url': 'https://www.ishares.com/us/products/239525/ishares-emerging-markets-corporate-bond-etf/1467271812596.ajax?fileType=csv&fileName=CEMB_holdings&dataType=fund',
        'header_row': 9
    },
    'EMBI': {
        'name': 'iShares J.P. Morgan USD Emerging Markets Bond ETF',
        'url': 'https://www.ishares.com/us/products/239572/ishares-jp-morgan-usd-emerging-markets-bond-etf/1467271812596.ajax?fileType=csv&fileName=EMB_holdings&dataType=fund',
        'header_row': 9
    },
    'GBI': {
        'name': 'iShares Emerging Markets Local Currency Bond ETF',
        'url': 'https://www.ishares.com/us/products/239528/ishares-emerging-markets-local-currency-bond-etf/1467271812596.ajax?fileType=csv&fileName=LEMB_holdings&dataType=fund',
        'header_row': 9
    },
    'EMHY': {
        'name': 'iShares Emerging Markets High Yield Bond ETF',
        'url': 'https://www.ishares.com/us/products/239527/ishares-emerging-markets-high-yield-bond-etf/1467271812596.ajax?fileType=csv&fileName=EMHY_holdings&dataType=fund',
        'header_row': 9
    }
}

# Database configuration
DB_PATH = 'data/etf_holdings.db'
DATA_DIR = 'data'

# ============================================================================
# DATABASE FUNCTIONS
# ============================================================================

def init_database():
    """Initialize SQLite database with holdings table"""
    os.makedirs(DATA_DIR, exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create holdings table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS holdings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            etf_code TEXT NOT NULL,
            date_of_pull DATE NOT NULL,
            ticker TEXT,
            name TEXT,
            location TEXT,
            sector TEXT,
            maturity TEXT,
            weight_pct REAL,
            ytm_pct REAL,
            market_value REAL,
            notional_value REAL,
            shares REAL,
            price REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(etf_code, date_of_pull, ticker, name)
        )
    ''')
    
    # Create indexes for fast queries
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON holdings(date_of_pull)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_etf_date ON holdings(etf_code, date_of_pull)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ticker ON holdings(ticker)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_location ON holdings(location)')
    
    conn.commit()
    conn.close()
    print(f"✓ Database initialized at {DB_PATH}")

def check_data_exists(etf_code, date_str):
    """Check if data for this ETF and date already exists"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT COUNT(*) FROM holdings 
        WHERE etf_code = ? AND date_of_pull = ?
    ''', (etf_code, date_str))
    
    count = cursor.fetchone()[0]
    conn.close()
    
    return count > 0

# ============================================================================
# ETF DATA FUNCTIONS
# ============================================================================

def fetch_etf_data(etf_code, config):
    """Fetch and clean data for a specific ETF"""
    try:
        print(f"\n📊 Fetching {etf_code}...")
        
        # Read CSV
        df = pd.read_csv(config['url'], header=config['header_row'])
        
        # Basic cleaning
        df = df[pd.to_numeric(df.get("Weight (%)", 0), errors="coerce") != 0].head(1000)
        
        # Convert numeric columns
        numeric_columns = {
            "Weight (%)": "weight_pct",
            "YTM (%)": "ytm_pct",
            "Market Value": "market_value",
            "Notional Value": "notional_value",
            "Shares": "shares",
            "Price": "price"
        }
        
        for orig_col, new_col in numeric_columns.items():
            if orig_col in df.columns:
                df[new_col] = pd.to_numeric(df[orig_col], errors="coerce")
        
        # Rename standard columns
        column_mapping = {
            "Ticker": "ticker",
            "Name": "name",
            "Location": "location",
            "Sector": "sector",
            "Maturity": "maturity"
        }
        
        df = df.rename(columns=column_mapping)
        
        # Add metadata
        df['etf_code'] = etf_code
        df['date_of_pull'] = datetime.now().strftime('%Y-%m-%d')
        
        # Select only columns we want to store
        columns_to_keep = [
            'etf_code', 'date_of_pull', 'ticker', 'name', 'location', 
            'sector', 'maturity', 'weight_pct', 'ytm_pct', 'market_value',
            'notional_value', 'shares', 'price'
        ]
        
        # Keep only columns that exist
        columns_to_keep = [col for col in columns_to_keep if col in df.columns]
        df = df[columns_to_keep]
        
        # Remove rows with too many nulls
        df = df.dropna(thresh=len(df.columns) * 0.5)
        
        print(f"  ✓ Fetched {len(df)} holdings")
        return df
        
    except Exception as e:
        print(f"  ✗ Error fetching {etf_code}: {str(e)}")
        return None

def save_to_database(df, etf_code):
    """Save dataframe to SQLite database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # Use INSERT OR IGNORE to avoid duplicates
        df.to_sql('holdings', conn, if_exists='append', index=False, method='multi')
        
        conn.commit()
        conn.close()
        
        print(f"  ✓ Saved {len(df)} records to database")
        return True
        
    except sqlite3.IntegrityError:
        print(f"  ⚠ Data already exists for this date, skipping...")
        return False
    except Exception as e:
        print(f"  ✗ Error saving {etf_code}: {str(e)}")
        return False

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def run_tracker(etf_codes=None):
    """
    Main function to run the ETF tracker
    
    Args:
        etf_codes: List of ETF codes to fetch. If None, fetches all configured ETFs
    """
    print("=" * 60)
    print("ETF HOLDINGS TRACKER")
    print("=" * 60)
    print(f"Run date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Initialize database
    init_database()
    
    # Determine which ETFs to fetch
    if etf_codes is None:
        etf_codes = list(ETF_CONFIG.keys())
    
    # Track results
    results = {
        'success': [],
        'failed': [],
        'skipped': []
    }
    
    # Fetch each ETF
    for etf_code in etf_codes:
        if etf_code not in ETF_CONFIG:
            print(f"\n⚠ {etf_code} not found in configuration, skipping...")
            results['failed'].append(etf_code)
            continue
        
        config = ETF_CONFIG[etf_code]
        today = datetime.now().strftime('%Y-%m-%d')
        
        # Check if already fetched today
        if check_data_exists(etf_code, today):
            print(f"\n⏭ {etf_code} already fetched today, skipping...")
            results['skipped'].append(etf_code)
            continue
        
        # Fetch and save
        df = fetch_etf_data(etf_code, config)
        
        if df is not None and len(df) > 0:
            success = save_to_database(df, etf_code)
            if success:
                results['success'].append(etf_code)
            else:
                results['skipped'].append(etf_code)
        else:
            results['failed'].append(etf_code)
    
    # Print summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"✓ Success: {len(results['success'])} - {', '.join(results['success']) if results['success'] else 'None'}")
    print(f"⏭ Skipped: {len(results['skipped'])} - {', '.join(results['skipped']) if results['skipped'] else 'None'}")
    print(f"✗ Failed:  {len(results['failed'])} - {', '.join(results['failed']) if results['failed'] else 'None'}")
    print("=" * 60)

if __name__ == "__main__":
    # Run tracker for all configured ETFs
    run_tracker()
    
    # Or run for specific ETFs only:
    # run_tracker(['EMBI', 'CEMBI'])
