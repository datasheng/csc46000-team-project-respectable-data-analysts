import os
import sys
import time
import pandas as pd
import psycopg2
from massive import RESTClient
from datetime import datetime
from zoneinfo import ZoneInfo
from tqdm import tqdm
from pathlib import Path

# Add parent directory to path to import config
sys.path.append(str(Path(__file__).parent.parent))
import config

# Initialize API client
if not config.MASSIVE_API_KEY:
    raise ValueError("MASSIVE_API_KEY not found in config")

client = RESTClient(api_key=config.MASSIVE_API_KEY)

# Helper functions (from api.ipynb)
def today():
    """Get current date in NY timezone"""
    return datetime.now(ZoneInfo("America/New_York")).date()

def parse_date(string):
    """Parse date string to date object"""
    return datetime.strptime(string, "%Y-%m-%d").date()

def clamp_date(end_date):
    """Ensure date is within 2-year API limit"""
    return end_date.replace(year=end_date.year - 2)

def convert(bars):
    """Convert API response to DataFrame"""
    cols = ["timestamp", "open", "high", "low", "close", "volume", "vwap"]
    if not bars:
        return pd.DataFrame(columns=cols)
    
    df = pd.DataFrame([vars(bar) for bar in bars])
    df = df.drop_duplicates(subset=["timestamp"], keep="first")
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    result = df[cols].sort_values("timestamp").reset_index(drop=True)
    
    return result

def fetch_data(client, ticker, start_date, end_date):
    """Fetch data from Massive API with rate limiting"""
    aggs = []
    current_start = start_date

    print(f"Grabbing {ticker} from {start_date.isoformat()} to {end_date.isoformat()}")
    with tqdm(desc="Fetching bars", unit="bars") as pbar:
        while current_start <= end_date:
            try:
                for bar in client.list_aggs(
                    ticker, config.MULTIPLIER, config.TIMESPAN,
                    current_start.isoformat(), end_date.isoformat(),
                    adjusted=config.ADJUSTED, sort=config.SORT, limit=config.LIMIT
                ):
                    aggs.append(bar)
                    pbar.update(1)
                break
            except Exception as e:
                if not aggs:
                    raise e
                last_bar = aggs[-1]
                last_date = datetime.fromtimestamp(last_bar.timestamp / 1000, tz=ZoneInfo("America/New_York")).date()
                time.sleep(config.SLEEP_TIME * 2)
                current_start = last_date

    return aggs

def create_db_connection():
    """Create database connection"""
    return psycopg2.connect(config.DB_CONNECTION_STRING)

def save_to_database(df, ticker, db_conn):
    """Save DataFrame to raw_market_data table"""
    if df.empty:
        print(f"  No data to save for {ticker}")
        return 0
    
    cursor = db_conn.cursor()
    rows_inserted = 0
    
    try:
        # Prepare data for insertion
        insert_query = """
            INSERT INTO raw_market_data (ticker, timestamp, open, high, low, close, volume, vwap)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticker, timestamp) DO NOTHING
        """
        
        data_to_insert = []
        for _, row in df.iterrows():
            data_to_insert.append((
                ticker,
                row['timestamp'],
                float(row['open']) if pd.notna(row['open']) else None,
                float(row['high']) if pd.notna(row['high']) else None,
                float(row['low']) if pd.notna(row['low']) else None,
                float(row['close']) if pd.notna(row['close']) else None,
                int(row['volume']) if pd.notna(row['volume']) else None,
                float(row['vwap']) if pd.notna(row['vwap']) else None
            ))
        
        # Batch insert
        cursor.executemany(insert_query, data_to_insert)
        rows_inserted = cursor.rowcount
        db_conn.commit()
        print(f"  ✓ Inserted {rows_inserted} rows for {ticker}")
        
    except Exception as e:
        db_conn.rollback()
        print(f"  ✗ Error saving {ticker} to database: {e}")
        raise
    finally:
        cursor.close()
    
    return rows_inserted

def fetch_ticker_to_db(ticker, db_conn):
    """Fetch data for a ticker and save to database"""
    end_date = today()
    allowed_start = clamp_date(end_date)
    start_date = allowed_start  # Use 2-year limit

    if start_date < allowed_start:
        print(f"  Error: {start_date} older than 2-year limit {allowed_start}")
        return 0

    try:
        # Fetch data from API
        bars = fetch_data(client, ticker, start_date, end_date)
        
        # Convert to DataFrame
        df = convert(bars)
        
        if df.empty:
            print(f"  No data returned for {ticker}")
            return 0
        
        # Save to database
        rows_inserted = save_to_database(df, ticker, db_conn)
        return rows_inserted
        
    except Exception as exc:
        print(f"  ✗ Error fetching {ticker}: {type(exc).__name__}: {exc}")
        return 0

def fetch_all_tickers(tickers, db_conn):
    """Fetch data for all tickers"""
    print(f"\n{'='*60}")
    print(f"Fetching data for {len(tickers)} tickers: {', '.join(tickers)}")
    print(f"{'='*60}\n")
    
    total_rows = 0
    for ticker in tickers:
        print(f"\nProcessing {ticker}...")
        rows = fetch_ticker_to_db(ticker, db_conn)
        total_rows += rows
    
    print(f"\n{'='*60}")
    print(f"✓ Completed! Total rows inserted: {total_rows}")
    print(f"{'='*60}")

if __name__ == "__main__":
    # Create database connection
    conn = create_db_connection()
    
    try:
        # Fetch data for all configured tickers
        fetch_all_tickers(config.TICKERS_TO_FETCH, conn)
    finally:
        conn.close()
        print("\nDatabase connection closed")

