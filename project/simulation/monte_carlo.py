import numpy as np
import pandas as pd
import psycopg2
import sys
from pathlib import Path

# add parent directory to path to import config.py
sys.path.append(str(Path(__file__).parent.parent))
import config

def create_db_connection():
    # create database connection
    return psycopg2.connect(config.DB_CONNECTION_STRING)

def load_historical_data(ticker, db_conn):
    # load historical data from processed_market_data table
    query = """
        SELECT date, close, return
        FROM processed_market_data
        WHERE ticker = %s
        ORDER BY date
    """
    
    df = pd.read_sql_query(query, db_conn, params=(ticker,))
    
    if df.empty:
        raise ValueError(f"No historical data found for {ticker}")
    
    # calculate returns if not already in database
    if 'return' not in df.columns or df['return'].isna().all():
        df['return'] = df['close'].pct_change()
        df = df.dropna()
    
    return df

def calculate_statistics(returns_series):
    # calculate annualized mean return and volatility from daily returns
    # mean daily return
    mean_daily = returns_series.mean()
    
    # standard deviation (volatility) of daily returns
    std_daily = returns_series.std()
    
    # annualize: mean * 252 trading days, std * sqrt(252)
    annual_mean = mean_daily * config.TRADING_DAYS_PER_YEAR
    annual_std = std_daily * np.sqrt(config.TRADING_DAYS_PER_YEAR)
    
    return annual_mean, annual_std

def get_starting_price(ticker, db_conn):
    # get the most recent closing price from database
    query = """
        SELECT close
        FROM processed_market_data
        WHERE ticker = %s
        ORDER BY date DESC
        LIMIT 1
    """
    
    cursor = db_conn.cursor()
    try:
        cursor.execute(query, (ticker,))
        result = cursor.fetchone()
        if result:
            return float(result[0])
        else:
            raise ValueError(f"No price data found for {ticker}")
    finally:
        cursor.close()

def simulate_single_path(ticker, initial_value, years, annual_mean, annual_std, db_conn):
    # simulate a single price path using geometric brownian motion
    # returns final portfolio value after simulation
    # get starting price
    starting_price = get_starting_price(ticker, db_conn)
    
    # calculate number of shares we can buy
    num_shares = initial_value / starting_price
    
    # number of trading days
    num_days = years * config.TRADING_DAYS_PER_YEAR
    
    # time step (one trading day)
    dt = 1.0 / config.TRADING_DAYS_PER_YEAR
    
    # initialize price
    price = starting_price
    
    # simulate price path using geometric brownian motion
    for _ in range(int(num_days)):
        # generate random num from standard normal distribution (mean = 0, std = 1)
        random_shock = np.random.normal(0, 1)
        
        # gbm formula: S(t+dt) = S(t) * exp((mu - 0.5*sigma^2)*dt + sigma*sqrt(dt)*Z)
        # used to update the price of the stock/etf
        price = price * np.exp(
            (annual_mean - 0.5 * annual_std**2) * dt + 
            annual_std * np.sqrt(dt) * random_shock
        )
    
    # final value = number of shares * final price (value of the portfolio)
    final_value = num_shares * price
    
    return final_value

def run_monte_carlo(ticker, initial_value, years, num_simulations, db_conn):
    # run monte carlo simulation for a single ticker
    # returns dataframe with columns: simulation_number, final_value, return_pct
    print(f"\nRunning {num_simulations} simulations for {ticker} over {years} years...")
    
    # load historical data
    df = load_historical_data(ticker, db_conn)
    print(f"  Loaded {len(df)} days of historical data")
    
    # calculate statistics
    annual_mean, annual_std = calculate_statistics(df['return'])
    print(f"  Annualized mean return: {annual_mean:.4%}")
    print(f"  Annualized volatility: {annual_std:.4%}")
    
    # run simulations
    results = []
    print(f"  Running simulations...")
    
    for sim_num in range(1, num_simulations + 1):
        final_value = simulate_single_path(
            ticker, initial_value, years, annual_mean, annual_std, db_conn
        )
        
        return_pct = ((final_value - initial_value) / initial_value) * 100
        
        results.append({
            'simulation_number': sim_num,
            'final_value': final_value,
            'return_pct': return_pct
        })
        
        # progress update every 1000 simulations
        if sim_num % 1000 == 0:
            print(f"    Completed {sim_num}/{num_simulations} simulations...")
    
    # create dataframe
    results_df = pd.DataFrame(results)
    
    print(f"  ---- Completed {num_simulations} simulations ----")
    print(f"  Mean final value: ${results_df['final_value'].mean():,.2f}")
    print(f"  Mean return: {results_df['return_pct'].mean():.2f}%")
    
    return results_df

def save_simulation_results(run_id, ticker, initial_value, results_df, db_conn):
    # save simulation results to database
    cursor = db_conn.cursor()
    
    try:
        insert_query = """
            INSERT INTO simulation_results (run_id, ticker, initial_value, final_value, return_pct, simulation_number)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        data_to_insert = []
        for _, row in results_df.iterrows():
            data_to_insert.append((
                run_id,
                ticker,
                float(initial_value),
                float(row['final_value']),
                float(row['return_pct']),
                int(row['simulation_number'])
            ))
        
        cursor.executemany(insert_query, data_to_insert)
        db_conn.commit()
        print(f"  ---- Saved {len(data_to_insert)} results to database ----")
        
    except Exception as e:
        db_conn.rollback()
        print(f"  ---- Error saving results: {e} ----")
        raise
    finally:
        cursor.close()

