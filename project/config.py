import os
from pathlib import Path
from dotenv import load_dotenv

# load environment variables from .env file
script_dir = Path(__file__).parent
env_path = script_dir / ".env"
load_dotenv(env_path)

# database configuration
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")

if not DB_CONNECTION_STRING:
    raise ValueError("DB_CONNECTION_STRING not found in .env file")

# api configuration
MASSIVE_API_KEY = os.getenv("MASSIVE_API_KEY")

# portfolio configuration
PORTFOLIO_A = {
    'AAPL': 125000,  # $125k in Apple
    'AMZN': 125000   # $125k in Amazon
}

PORTFOLIO_B = {
    'XLK': 125000,   # $125k in Technology ETF
    'XLF': 125000   # $125k in Financial ETF
}

INITIAL_INVESTMENT = 250000  # Total investment per portfolio

# simulation parameters
NUM_SIMULATIONS = 10000
TIME_HORIZONS = [10, 20]  # years
TRADING_DAYS_PER_YEAR = 252

# tickers to fetch (for etl pipeline)
TICKERS_TO_FETCH = ['AAPL', 'AMZN', 'XLK', 'XLF']

# data fetching parameters (from api.ipynb)
MULTIPLIER = 1
TIMESPAN = "day"  # changed from "hour" to "day" for daily data
ADJUSTED = "true"
SORT = "asc"
LIMIT = 50000
SLEEP_TIME = 60/5  # rate limit is 5/min

