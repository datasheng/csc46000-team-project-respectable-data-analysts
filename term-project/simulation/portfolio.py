import pandas as pd
import sys
from pathlib import Path

# add parent directory to path to import modules
sys.path.append(str(Path(__file__).parent.parent))
from simulation.monte_carlo import run_monte_carlo, save_simulation_results, create_db_connection
import config

def simulate_portfolio(portfolio_dict, years, num_simulations, db_conn, run_id=None):
    # simulate a portfolio by combining individual ticker simulations
    # portfolio_dict: dictionary of {ticker: allocation_amount}
    # returns dataframe with combined portfolio results across all tickers
    print(f"\n{'='*60}")
    print(f"Simulating portfolio with {len(portfolio_dict)} tickers over {years} years")
    print(f"{'='*60}")
    
    # store individual ticker results
    ticker_results = {}
    
    # run simulation for each ticker in portfolio
    for ticker, allocation in portfolio_dict.items():
        print(f"\n--- Simulating {ticker} (${allocation:,.2f}) ---")
        
        # run monte carlo for this ticker
        results_df = run_monte_carlo(ticker, allocation, years, num_simulations, db_conn)
        ticker_results[ticker] = results_df
        
        # save individual ticker results if run_id provided
        if run_id:
            save_simulation_results(run_id, ticker, allocation, results_df, db_conn)
    
    # for each simulation number, sum final_values across all tickers
    print(f"\n--- Combining portfolio results ---")
    
    portfolio_results = []
    for sim_num in range(1, num_simulations + 1):
        portfolio_final_value = 0
        
        # sum final values across all tickers for this simulation
        for ticker, results_df in ticker_results.items():
            ticker_result = results_df[results_df['simulation_number'] == sim_num]
            if not ticker_result.empty:
                portfolio_final_value += ticker_result.iloc[0]['final_value']
        
        # calculate portfolio return
        portfolio_initial = sum(portfolio_dict.values())
        portfolio_return_pct = ((portfolio_final_value - portfolio_initial) / portfolio_initial) * 100
        
        portfolio_results.append({
            'simulation_number': sim_num,
            'final_value': portfolio_final_value,
            'return_pct': portfolio_return_pct
        })
    
    portfolio_df = pd.DataFrame(portfolio_results)
    
    print(f"  ---- Combined {num_simulations} portfolio simulations ----")
    print(f"  Mean portfolio final value: ${portfolio_df['final_value'].mean():,.2f}")
    print(f"  Mean portfolio return: {portfolio_df['return_pct'].mean():.2f}%")
    
    return portfolio_df

def save_portfolio_results(run_id, portfolio_type, portfolio_results_df, db_conn):
    # save portfolio results to database
    cursor = db_conn.cursor()
    
    try:
        insert_query = """
            INSERT INTO portfolio_results (run_id, portfolio_type, final_value, return_pct, simulation_number)
            VALUES (%s, %s, %s, %s, %s)
        """
        
        data_to_insert = []
        for _, row in portfolio_results_df.iterrows():
            data_to_insert.append((
                run_id,
                portfolio_type,
                float(row['final_value']),
                float(row['return_pct']),
                int(row['simulation_number'])
            ))
        
        cursor.executemany(insert_query, data_to_insert)
        db_conn.commit()
        print(f"  ---- Saved {len(data_to_insert)} portfolio results to database ----")
        
    except Exception as e:
        db_conn.rollback()
        print(f"  ---- Error saving portfolio results: {e} ----")
        raise
    finally:
        cursor.close()

def calculate_summary_statistics(results_df):
    # calculate summary statistics from simulation results
    stats = {
        'mean': results_df['final_value'].mean(),
        'median': results_df['final_value'].median(),
        'std': results_df['final_value'].std(),
        'min': results_df['final_value'].min(),
        'max': results_df['final_value'].max(),
        'p5': results_df['final_value'].quantile(0.05),
        'p25': results_df['final_value'].quantile(0.25),
        'p75': results_df['final_value'].quantile(0.75),
        'p95': results_df['final_value'].quantile(0.95)
    }
    
    # also calculate return statistics
    stats['mean_return'] = results_df['return_pct'].mean()
    stats['median_return'] = results_df['return_pct'].median()
    stats['std_return'] = results_df['return_pct'].std()
    stats['min_return'] = results_df['return_pct'].min()
    stats['max_return'] = results_df['return_pct'].max()
    
    return stats

def save_summary_statistics(run_id, portfolio_type, stats_dict, db_conn):
    # save summary statistics to database
    cursor = db_conn.cursor()
    
    try:
        insert_query = """
            INSERT INTO simulation_summary (run_id, portfolio_type, metric_name, metric_value)
            VALUES (%s, %s, %s, %s)
        """
        
        data_to_insert = []
        for metric_name, metric_value in stats_dict.items():
            data_to_insert.append((
                run_id,
                portfolio_type,
                metric_name,
                float(metric_value)
            ))
        
        cursor.executemany(insert_query, data_to_insert)
        db_conn.commit()
        print(f"  ---- Saved {len(data_to_insert)} summary statistics to database ----")
        
    except Exception as e:
        db_conn.rollback()
        print(f"  ---- Error saving summary statistics: {e} ----")
        raise
    finally:
        cursor.close()

