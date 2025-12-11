import sys
from pathlib import Path
from datetime import datetime

# add parent directory to path to import modules
sys.path.append(str(Path(__file__).parent.parent))
from simulation.portfolio import (
    simulate_portfolio, 
    save_portfolio_results, 
    calculate_summary_statistics, 
    save_summary_statistics,
    create_db_connection
)
import config

def create_simulation_run(portfolio_type, time_horizon, num_simulations, db_conn):
    # create a simulation run record and return run_id
    cursor = db_conn.cursor()
    
    try:
        insert_query = """
            INSERT INTO simulation_runs (portfolio_type, time_horizon_years, num_simulations)
            VALUES (%s, %s, %s)
            RETURNING run_id
        """
        
        cursor.execute(insert_query, (portfolio_type, time_horizon, num_simulations))
        run_id = cursor.fetchone()[0]
        db_conn.commit()
        
        return run_id
        
    except Exception as e:
        db_conn.rollback()
        print(f"---- Error creating simulation run: {e} ----")
        raise
    finally:
        cursor.close()

def run_all_simulations(db_conn):
    # run all simulations for both portfolios and both time horizons
    print("=" * 70)
    print("MONTE CARLO SIMULATION - INVESTMENT PORTFOLIO ANALYSIS")
    print("=" * 70)
    print(f"\nConfiguration:")
    print(f"  Portfolios: A (Stocks: AAPL + AMZN) vs B (ETFs: XLK + XLF)")
    print(f"  Initial Investment: ${config.INITIAL_INVESTMENT:,} per portfolio")
    print(f"  Number of Simulations: {config.NUM_SIMULATIONS:,}")
    print(f"  Time Horizons: {config.TIME_HORIZONS} years")
    print("=" * 70)
    
    start_time = datetime.now()
    
    # run simulations for each time horizon
    for years in config.TIME_HORIZONS:
        print(f"\n\n{'#'*70}")
        print(f"# TIME HORIZON: {years} YEARS")
        print(f"{'#'*70}\n")
        
        # portfolio A (stocks)
        print(f"\n{'='*70}")
        print(f"PORTFOLIO A: STOCKS (AAPL + AMZN)")
        print(f"{'='*70}")
        
        portfolio_a_id = create_simulation_run('A', years, config.NUM_SIMULATIONS, db_conn)
        print(f"Created simulation run ID: {portfolio_a_id}")
        
        # simulate portfolio (saves individual ticker results automatically)
        portfolio_a_results = simulate_portfolio(
            config.PORTFOLIO_A, years, config.NUM_SIMULATIONS, db_conn, run_id=portfolio_a_id
        )
        
        # save portfolio results
        save_portfolio_results(portfolio_a_id, 'A', portfolio_a_results, db_conn)
        
        # calculate and save summary statistics
        portfolio_a_stats = calculate_summary_statistics(portfolio_a_results)
        save_summary_statistics(portfolio_a_id, 'A', portfolio_a_stats, db_conn)
        
        # portfolio B (etfs)
        print(f"\n\n{'='*70}")
        print(f"PORTFOLIO B: ETFs (XLK + XLF)")
        print(f"{'='*70}")
        
        portfolio_b_id = create_simulation_run('B', years, config.NUM_SIMULATIONS, db_conn)
        print(f"Created simulation run ID: {portfolio_b_id}")
        
        # simulate portfolio (saves individual ticker results automatically)
        portfolio_b_results = simulate_portfolio(
            config.PORTFOLIO_B, years, config.NUM_SIMULATIONS, db_conn, run_id=portfolio_b_id
        )
        
        # save portfolio results
        save_portfolio_results(portfolio_b_id, 'B', portfolio_b_results, db_conn)
        
        # calculate and save summary statistics
        portfolio_b_stats = calculate_summary_statistics(portfolio_b_results)
        save_summary_statistics(portfolio_b_id, 'B', portfolio_b_stats, db_conn)
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    print(f"\n\n{'='*70}")
    print("SIMULATION COMPLETE!")
    print(f"{'='*70}")
    print(f"Total execution time: {duration}")
    print(f"Results saved to database")
    print(f"{'='*70}")

if __name__ == "__main__":
    conn = create_db_connection()
    
    try:
        run_all_simulations(conn)
    except Exception as e:
        print(f"\n---- Simulation failed: {e} ----")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()
        print("\n---- Database connection closed ----")

