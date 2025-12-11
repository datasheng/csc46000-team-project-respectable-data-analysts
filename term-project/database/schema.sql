-- Raw Market Data Table (stores data directly from API)
CREATE TABLE IF NOT EXISTS raw_market_data (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    open NUMERIC(15, 4),
    high NUMERIC(15, 4),
    low NUMERIC(15, 4),
    close NUMERIC(15, 4),
    volume BIGINT,
    vwap NUMERIC(15, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ticker, timestamp)
);

-- Processed Market Data Table (stores feature-engineered data)
CREATE TABLE IF NOT EXISTS processed_market_data (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    -- Original price data
    open NUMERIC(15, 4),
    high NUMERIC(15, 4),
    low NUMERIC(15, 4),
    close NUMERIC(15, 4),
    volume BIGINT,
    vwap NUMERIC(15, 4),
    -- Time-based features
    year INTEGER,
    month INTEGER,
    day INTEGER,
    weekday INTEGER,
    is_month_start INTEGER,
    is_month_end INTEGER,
    -- Price & volume features
    high_low_range NUMERIC(15, 4),
    average_price NUMERIC(15, 4),
    volume_change NUMERIC(15, 8),
    -- Lag & return features
    close_lag_1 NUMERIC(15, 4),
    close_lag_2 NUMERIC(15, 4),
    return NUMERIC(15, 8),
    return_lag_1 NUMERIC(15, 8),
    -- Rolling statistics
    rolling_mean_7 NUMERIC(15, 4),
    rolling_std_7 NUMERIC(15, 8),
    rolling_mean_30 NUMERIC(15, 4),
    rolling_std_30 NUMERIC(15, 8),
    -- Moving averages
    ma14 NUMERIC(15, 8),
    ma30 NUMERIC(15, 8),
    ma50 NUMERIC(15, 8),
    ma200 NUMERIC(15, 8),
    -- RSI
    rsi14 NUMERIC(10, 4),
    rsi30 NUMERIC(10, 4),
    rsi50 NUMERIC(10, 4),
    -- Rate of change
    roc14 NUMERIC(15, 8),
    -- Volatility
    vol14 NUMERIC(15, 8),
    -- Up/down day flags
    up_day INTEGER,
    down_day INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ticker, date)
);

-- Simulation Runs Table (tracks each simulation execution)
CREATE TABLE IF NOT EXISTS simulation_runs (
    run_id SERIAL PRIMARY KEY,
    portfolio_type VARCHAR(10) NOT NULL,  -- 'A' or 'B'
    time_horizon_years INTEGER NOT NULL,  -- 10 or 20
    num_simulations INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Simulation Results Table (individual ticker results per simulation)
CREATE TABLE IF NOT EXISTS simulation_results (
    result_id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES simulation_runs(run_id) ON DELETE CASCADE,
    ticker VARCHAR(10) NOT NULL,
    initial_value NUMERIC(15, 2),
    final_value NUMERIC(15, 2),
    return_pct NUMERIC(10, 4),
    simulation_number INTEGER NOT NULL
);

-- Portfolio Results Table (combined portfolio results per simulation)
CREATE TABLE IF NOT EXISTS portfolio_results (
    portfolio_result_id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES simulation_runs(run_id) ON DELETE CASCADE,
    portfolio_type VARCHAR(10) NOT NULL,
    final_value NUMERIC(15, 2),
    return_pct NUMERIC(10, 4),
    simulation_number INTEGER NOT NULL
);

-- Simulation Summary Table (aggregated statistics per portfolio)
CREATE TABLE IF NOT EXISTS simulation_summary (
    summary_id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES simulation_runs(run_id) ON DELETE CASCADE,
    portfolio_type VARCHAR(10) NOT NULL,
    metric_name VARCHAR(50) NOT NULL,  -- 'mean', 'median', 'std', 'min', 'max', 'p5', 'p25', 'p75', 'p95'
    metric_value NUMERIC(15, 4)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_raw_market_data_ticker_timestamp ON raw_market_data(ticker, timestamp);
CREATE INDEX IF NOT EXISTS idx_processed_market_data_ticker_date ON processed_market_data(ticker, date);
CREATE INDEX IF NOT EXISTS idx_simulation_results_run_id ON simulation_results(run_id);
CREATE INDEX IF NOT EXISTS idx_portfolio_results_run_id ON portfolio_results(run_id);
CREATE INDEX IF NOT EXISTS idx_simulation_summary_run_id ON simulation_summary(run_id);

