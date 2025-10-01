"""
Query Helper for ETF Holdings Database
Easy functions to analyze your historical data
"""

import pandas as pd
import sqlite3
from datetime import datetime, timedelta

DB_PATH = 'data/etf_holdings.db'

# ============================================================================
# SIMPLE QUERY FUNCTIONS
# ============================================================================

def get_latest_holdings(etf_code):
    """Get the most recent holdings for an ETF"""
    conn = sqlite3.connect(DB_PATH)
    
    query = f"""
        SELECT * FROM holdings 
        WHERE etf_code = '{etf_code}'
        AND date_of_pull = (
            SELECT MAX(date_of_pull) FROM holdings WHERE etf_code = '{etf_code}'
        )
        ORDER BY weight_pct DESC
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print(f"📊 Latest holdings for {etf_code}: {df['date_of_pull'].iloc[0] if len(df) > 0 else 'No data'}")
    return df

def get_holdings_by_date(etf_code, date_str):
    """Get holdings for a specific date (format: 'YYYY-MM-DD')"""
    conn = sqlite3.connect(DB_PATH)
    
    query = f"""
        SELECT * FROM holdings 
        WHERE etf_code = '{etf_code}'
        AND date_of_pull = '{date_str}'
        ORDER BY weight_pct DESC
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print(f"📊 Holdings for {etf_code} on {date_str}: {len(df)} records")
    return df

def get_top_holdings(etf_code, top_n=10, date_str=None):
    """Get top N holdings by weight"""
    if date_str is None:
        df = get_latest_holdings(etf_code)
    else:
        df = get_holdings_by_date(etf_code, date_str)
    
    return df.head(top_n)[['name', 'location', 'weight_pct', 'ytm_pct', 'maturity']]

def track_bond_over_time(ticker=None, name=None, etf_code=None):
    """Track a specific bond's metrics over time"""
    conn = sqlite3.connect(DB_PATH)
    
    where_clauses = []
    if ticker:
        where_clauses.append(f"ticker = '{ticker}'")
    if name:
        where_clauses.append(f"name LIKE '%{name}%'")
    if etf_code:
        where_clauses.append(f"etf_code = '{etf_code}'")
    
    where_str = " AND ".join(where_clauses) if where_clauses else "1=1"
    
    query = f"""
        SELECT date_of_pull, etf_code, name, ticker, location,
               weight_pct, ytm_pct, price, market_value
        FROM holdings 
        WHERE {where_str}
        ORDER BY date_of_pull, etf_code
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print(f"📈 Found {len(df)} records over {df['date_of_pull'].nunique()} dates")
    return df

def get_country_exposure_over_time(etf_code, location):
    """Track a country's total weight over time"""
    conn = sqlite3.connect(DB_PATH)
    
    query = f"""
        SELECT date_of_pull, 
               SUM(weight_pct) as total_weight,
               COUNT(*) as num_holdings,
               AVG(ytm_pct) as avg_ytm
        FROM holdings 
        WHERE etf_code = '{etf_code}'
        AND location = '{location}'
        GROUP BY date_of_pull
        ORDER BY date_of_pull
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print(f"🌍 {location} exposure in {etf_code} over time")
    return df

def compare_dates(etf_code, date1, date2):
    """Compare holdings between two dates"""
    df1 = get_holdings_by_date(etf_code, date1)
    df2 = get_holdings_by_date(etf_code, date2)
    
    # Find bonds that changed weight significantly
    merged = df1.merge(
        df2, 
        on=['ticker', 'name'], 
        suffixes=('_old', '_new'),
        how='outer'
    )
    
    merged['weight_change'] = merged['weight_pct_new'].fillna(0) - merged['weight_pct_old'].fillna(0)
    merged['ytm_change'] = merged['ytm_pct_new'] - merged['ytm_pct_old']
    
    # Sort by absolute weight change
    merged['abs_weight_change'] = merged['weight_change'].abs()
    merged = merged.sort_values('abs_weight_change', ascending=False)
    
    print(f"\n📊 Comparison: {date1} vs {date2}")
    print(f"  New holdings: {merged['weight_pct_old'].isna().sum()}")
    print(f"  Removed holdings: {merged['weight_pct_new'].isna().sum()}")
    print(f"  Changed holdings: {(merged['weight_change'] != 0).sum()}")
    
    return merged[['name', 'ticker', 'location_new', 'weight_pct_old', 'weight_pct_new', 
                   'weight_change', 'ytm_pct_old', 'ytm_pct_new', 'ytm_change']]

def get_available_dates(etf_code=None):
    """Show all dates we have data for"""
    conn = sqlite3.connect(DB_PATH)
    
    if etf_code:
        query = f"""
            SELECT DISTINCT date_of_pull, COUNT(*) as num_holdings
            FROM holdings 
            WHERE etf_code = '{etf_code}'
            GROUP BY date_of_pull
            ORDER BY date_of_pull DESC
        """
    else:
        query = """
            SELECT date_of_pull, etf_code, COUNT(*) as num_holdings
            FROM holdings 
            GROUP BY date_of_pull, etf_code
            ORDER BY date_of_pull DESC, etf_code
        """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    return df

def get_database_stats():
    """Get overall database statistics"""
    conn = sqlite3.connect(DB_PATH)
    
    stats = {}
    
    # Total records
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM holdings")
    stats['total_records'] = cursor.fetchone()[0]
    
    # Date range
    cursor.execute("SELECT MIN(date_of_pull), MAX(date_of_pull) FROM holdings")
    stats['date_range'] = cursor.fetchone()
    
    # Records by ETF
    df = pd.read_sql_query("""
        SELECT etf_code, COUNT(*) as records, 
               COUNT(DISTINCT date_of_pull) as dates,
               MIN(date_of_pull) as first_date,
               MAX(date_of_pull) as last_date
        FROM holdings 
        GROUP BY etf_code
    """, conn)
    
    conn.close()
    
    print("\n" + "=" * 60)
    print("DATABASE STATISTICS")
    print("=" * 60)
    print(f"Total Records: {stats['total_records']:,}")
    print(f"Date Range: {stats['date_range'][0]} to {stats['date_range'][1]}")
    print("\nBy ETF:")
    print(df.to_string(index=False))
    print("=" * 60)
    
    return stats

# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    print("ETF Query Helper - Examples\n")
    
    # Show what data we have
    print("\n1. Database Overview:")
    get_database_stats()
    
    print("\n2. Available dates:")
    print(get_available_dates())
    
    print("\n3. Latest EMBI holdings (top 10):")
    print(get_top_holdings('EMBI', top_n=10))
    
    # Uncomment these as you collect more data:
    
    # print("\n4. Track a specific bond over time:")
    # print(track_bond_over_time(name='MEXICO', etf_code='EMBI'))
    
    # print("\n5. Mexico exposure in EMBI over time:")
    # print(get_country_exposure_over_time('EMBI', 'MEXICO'))
    
    # print("\n6. Compare two dates:")
    # print(compare_dates('EMBI', '2025-01-01', '2025-01-15').head(20))
