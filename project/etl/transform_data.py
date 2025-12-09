import pandas as pd
import numpy as np
import psycopg2
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from processing import engineer_features
import config

def create_db_connection():
    """Create database connection"""
    return psycopg2.connect(config.DB_CONNECTION_STRING)

def load_raw_data(ticker, db_conn):
    """Load raw market data from database for a ticker"""
    query = """
        SELECT ticker, timestamp, open, high, low, close, volume, vwap
        FROM raw_market_data
        WHERE ticker = %s
        ORDER BY timestamp
    """
    
    df = pd.read_sql_query(query, db_conn, params=(ticker,))
    
    if df.empty:
        print(f"  No raw data found for {ticker}")
        return df
    
    # Rename timestamp to date for engineer_features
    # Keep as datetime (engineer_features will handle it)
    df['date'] = pd.to_datetime(df['timestamp'])
    df = df.drop('timestamp', axis=1)
    
    return df

def save_processed_data(df, ticker, db_conn):
    """Save processed DataFrame to processed_market_data table"""
    if df.empty:
        print(f"  No processed data to save for {ticker}")
        return 0
    
    cursor = db_conn.cursor()
    rows_inserted = 0
    
    try:
        # Prepare insert query with all columns
        insert_query = """
            INSERT INTO processed_market_data (
                ticker, date, open, high, low, close, volume, vwap,
                year, month, day, weekday, is_month_start, is_month_end,
                high_low_range, average_price, volume_change,
                close_lag_1, close_lag_2, return, return_lag_1,
                rolling_mean_7, rolling_std_7, rolling_mean_30, rolling_std_30,
                ma14, ma30, ma50, ma200,
                rsi14, rsi30, rsi50,
                roc14, vol14,
                up_day, down_day
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s
            )
            ON CONFLICT (ticker, date) DO NOTHING
        """
        
        data_to_insert = []
        for _, row in df.iterrows():
            # Convert date to date object if it's a datetime
            date_val = row['date']
            if pd.isna(date_val):
                continue
            if isinstance(date_val, pd.Timestamp):
                date_val = date_val.date()
            elif isinstance(date_val, str):
                from datetime import datetime
                date_val = datetime.strptime(date_val, '%Y-%m-%d').date()
            
            data_to_insert.append((
                ticker,
                date_val,
                float(row['open']) if pd.notna(row['open']) else None,
                float(row['high']) if pd.notna(row['high']) else None,
                float(row['low']) if pd.notna(row['low']) else None,
                float(row['close']) if pd.notna(row['close']) else None,
                int(row['volume']) if pd.notna(row['volume']) else None,
                float(row['vwap']) if pd.notna(row['vwap']) else None,
                # Time-based features
                int(row['year']) if pd.notna(row['year']) else None,
                int(row['month']) if pd.notna(row['month']) else None,
                int(row['day']) if pd.notna(row['day']) else None,
                int(row['weekday']) if pd.notna(row['weekday']) else None,
                int(row['is_month_start']) if pd.notna(row['is_month_start']) else None,
                int(row['is_month_end']) if pd.notna(row['is_month_end']) else None,
                # Price & volume features
                float(row['high_low_range']) if pd.notna(row['high_low_range']) else None,
                float(row['average_price']) if pd.notna(row['average_price']) else None,
                float(row['volume_change']) if pd.notna(row['volume_change']) else None,
                # Lag & return features
                float(row['close_lag_1']) if pd.notna(row['close_lag_1']) else None,
                float(row['close_lag_2']) if pd.notna(row['close_lag_2']) else None,
                float(row['return']) if pd.notna(row['return']) else None,
                float(row['return_lag_1']) if pd.notna(row['return_lag_1']) else None,
                # Rolling statistics
                float(row['rolling_mean_7']) if pd.notna(row['rolling_mean_7']) else None,
                float(row['rolling_std_7']) if pd.notna(row['rolling_std_7']) else None,
                float(row['rolling_mean_30']) if pd.notna(row['rolling_mean_30']) else None,
                float(row['rolling_std_30']) if pd.notna(row['rolling_std_30']) else None,
                # Moving averages
                float(row['ma14']) if pd.notna(row['ma14']) else None,
                float(row['ma30']) if pd.notna(row['ma30']) else None,
                float(row['ma50']) if pd.notna(row['ma50']) else None,
                float(row['ma200']) if pd.notna(row['ma200']) else None,
                # RSI
                float(row['rsi14']) if pd.notna(row['rsi14']) else None,
                float(row['rsi30']) if pd.notna(row['rsi30']) else None,
                float(row['rsi50']) if pd.notna(row['rsi50']) else None,
                # Rate of change
                float(row['roc14']) if pd.notna(row['roc14']) else None,
                # Volatility
                float(row['vol14']) if pd.notna(row['vol14']) else None,
                # Up/down day flags
                int(row['up_day']) if pd.notna(row['up_day']) else None,
                int(row['down_day']) if pd.notna(row['down_day']) else None,
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

def transform_ticker(ticker, db_conn):
    """Transform data for a single ticker"""
    print(f"\nProcessing {ticker}...")
    
    try:
        # Load raw data
        df = load_raw_data(ticker, db_conn)
        
        if df.empty:
            print(f"  No data to process for {ticker}")
            return 0
        
        print(f"  Loaded {len(df)} rows from raw_market_data")
        
        # Engineer features using existing function
        df = engineer_features(df)
        
        # Fill any remaining NaN values with 0 (as in original processing.py)
        df = df.fillna(0)
        
        print(f"  Engineered features, {len(df)} rows after processing")
        
        # Save processed data
        rows_inserted = save_processed_data(df, ticker, db_conn)
        
        return rows_inserted
        
    except Exception as e:
        print(f"  ✗ Error processing {ticker}: {type(e).__name__}: {e}")
        raise

def transform_all_tickers(tickers, db_conn):
    """Transform data for all tickers"""
    print(f"\n{'='*60}")
    print(f"Transforming data for {len(tickers)} tickers: {', '.join(tickers)}")
    print(f"{'='*60}\n")
    
    total_rows = 0
    for ticker in tickers:
        try:
            rows = transform_ticker(ticker, db_conn)
            total_rows += rows
        except Exception as e:
            print(f"  Failed to process {ticker}: {e}")
            continue
    
    print(f"\n{'='*60}")
    print(f"✓ Completed! Total rows inserted: {total_rows}")
    print(f"{'='*60}")

if __name__ == "__main__":
    # Create database connection
    conn = create_db_connection()
    
    try:
        # Transform data for all configured tickers
        transform_all_tickers(config.TICKERS_TO_FETCH, conn)
    finally:
        conn.close()
        print("\nDatabase connection closed")

