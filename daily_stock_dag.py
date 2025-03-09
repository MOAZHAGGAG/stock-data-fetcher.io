from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import trino
import logging
import time
import os
from datetime import datetime
import pytz
from airflow.timetables.trigger import CronTriggerTimetable
from pendulum import timezone

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Trino connection settings from environment variables
TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "airflow")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "stock_db")
COMPANY_TABLE = os.getenv("COMPANY_TABLE", "company_info")
TRINO_TABLE = os.getenv("TRINO_TABLE", "daily_stock_prices")

# Batch size and retry configuration
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "12"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "10"))
API_RATE_LIMIT_DELAY = int(os.getenv("API_RATE_LIMIT_DELAY", "2"))

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': True,
    'email_on_retry': False,
}

# Define the DAG using a standard cron expression
dag = DAG(
    'daily_stock_prices_from_yfinance_v2',
    default_args=default_args,
    description='Fetch latest stock prices with accurate timestamps from yfinance',
    schedule_interval='0 14 * * 0-4',  # Run once a day at 4:00 pm, Sunday-Thursday
    catchup=False,
)

def get_trino_connection():
    """Create and return a Trino connection."""
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA
    )

def get_company_data_from_trino():
    """Fetch the list of tickers and company codes from the company_info table in Iceberg."""
    conn = None
    cursor = None
    try:
        conn = get_trino_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT ticker, company_code FROM {COMPANY_TABLE}")
        company_data = {row[0]: row[1] for row in cursor.fetchall()}
        logger.info(f"Fetched {len(company_data)} companies from {COMPANY_TABLE}")
        return company_data
    except Exception as e:
        logger.error(f"Error fetching company data: {e}", exc_info=True)
        return {}
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def validate_stock_data(ticker, price, trade_date, company_code, current_datetime):
    """Validate stock data before inserting into database."""
    if not ticker or not isinstance(ticker, str):
        return False, "Invalid ticker symbol"
    
    if price is None or not isinstance(price, (int, float)) or price <= 0:
        return False, f"Invalid price for {ticker}: {price}"
    
    if not trade_date:
        return False, f"Invalid trade timestamp for {ticker}"
    
    if not current_datetime:
        return False, f"Invalid current datetime for {ticker}"
    
    if not company_code:
        return False, f"Missing company code for {ticker}"
        
    return True, None

def fetch_realtime_stock_data(ticker, company_code, max_retries=MAX_RETRIES, retry_delay=RETRY_DELAY):
    """Fetch latest stock data with timestamp."""
    egypt_tz = pytz.timezone('Africa/Cairo')
    
    for attempt in range(max_retries):
        try:
            # Get latest data
            stock = yf.Ticker(ticker)
            
            # Use only quote data
            quote_info = stock.info
            if 'regularMarketPrice' in quote_info and quote_info['regularMarketPrice'] is not None:
                current_price = round(quote_info['regularMarketPrice'], 2)
                
                # Get timestamp and convert to Egypt time
                if 'regularMarketTime' in quote_info and quote_info['regularMarketTime'] is not None:
                    # Convert Unix timestamp to datetime, then to Egypt time
                    utc_time = datetime.fromtimestamp(quote_info['regularMarketTime'], pytz.UTC)
                    egypt_time = utc_time.astimezone(egypt_tz)
                    trade_date = egypt_time.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    # Use current time in Egypt as fallback
                    egypt_time = datetime.now(egypt_tz)
                    trade_date = egypt_time.strftime("%Y-%m-%d %H:%M:%S")
                
                # Get current datetime in Egypt time with seconds precision
                current_datetime = datetime.now(egypt_tz).strftime("%Y-%m-%d %H:%M:%S")
                
                logger.info(f"Retrieved quote data for {ticker}(code: {company_code}): price={current_price} at {trade_date}, current datetime={current_datetime}")
                
                # Validate the data
                valid, error_msg = validate_stock_data(ticker, current_price, trade_date, company_code, current_datetime)
                if not valid:
                    logger.warning(f"Data validation failed: {error_msg}")
                    return None, None, None, None, None, error_msg
                    
                return trade_date, current_price, ticker, company_code, current_datetime, None
            else:
                logger.warning(f"No price available for {ticker}")
                return None, None, None, None, None, "No price available"
        except Exception as e:
            logger.warning(f"Attempt {attempt+1}/{max_retries} failed for {ticker}: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                return None, None, None, None, None, str(e)

def process_ticker_batch(tickers_with_codes):
    """Process a batch of tickers and return the values list."""
    values_list = []
    errors = []

    for ticker, company_code in tickers_with_codes.items():
        logger.info(f"Fetching latest price for {ticker} (company code: {company_code})...")
        trade_date, price, ticker_symbol, company_code, current_datetime, error = fetch_realtime_stock_data(ticker, company_code)
        
        if error:
            errors.append(f"{ticker}: {error}")
        else:
            # Order: fetching_date (TIMESTAMP), trade_date, company_code, ticker, price
            values_list.append(f"(TIMESTAMP '{current_datetime}', TIMESTAMP '{trade_date}', '{company_code}', '{ticker_symbol}', {price})")
            logger.info(f"Fetched {ticker_symbol} - {price} at {trade_date}, company code: {company_code}, current datetime: {current_datetime}")
        
        # Minimal delay to respect API rate limits
        time.sleep(API_RATE_LIMIT_DELAY)

    if errors:
        logger.warning(f"Errors occurred while processing tickers: {', '.join(errors)}")
        
    return values_list

def insert_into_trino(values_list):
    """Insert data into Trino/Iceberg table."""
    if not values_list:
        logger.warning("No data to insert into Iceberg.")
        return False

    conn = None
    cursor = None
    try:
        conn = get_trino_connection()
        cursor = conn.cursor()
        # Updated column order: fetching_date (TIMESTAMP), trade_date, company_code, ticker, price
        query = f"INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE} (fetching_date, trade_date, company_code, ticker, price) VALUES {', '.join(values_list)}"
        cursor.execute(query)
        logger.info(f"Data saved to Iceberg table '{TRINO_SCHEMA}.{TRINO_TABLE}', {len(values_list)} rows")
        return True
    except Exception as e:
        logger.error(f"Error inserting data into Trino: {e}", exc_info=True)
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def process_tickers_in_batches(company_data, batch_size=BATCH_SIZE):
    """Process tickers in batches to reduce memory usage and improve reliability."""
    all_values_list = []
    
    # Convert to list of items for batching
    items = list(company_data.items())
    
    # Process tickers in batches
    for i in range(0, len(items), batch_size):
        batch_items = items[i:i+batch_size]
        batch_dict = dict(batch_items)
        logger.info(f"Processing batch {i//batch_size + 1}/{(len(items) + batch_size - 1)//batch_size} with {len(batch_dict)} tickers")
        
        values_list = process_ticker_batch(batch_dict)
        all_values_list.extend(values_list)
    
    return all_values_list

def fetch_realtime_stock_prices():
    """
    Fetch latest stock prices with accurate timestamps from yfinance and store in Iceberg.
    """
    start_time = time.time()
    logger.info("Starting latest stock price fetch task with accurate timestamps")
    
    # Get tickers and company codes from Trino
    company_data = get_company_data_from_trino()
    if not company_data:
        logger.warning("No company data found, skipping stock price fetching.")
        return
    
    logger.info(f"Processing {len(company_data)} companies for latest data with accurate timestamps")
    
    try:
        # Process tickers in batches
        all_values_list = process_tickers_in_batches(company_data)
        
        # Save to Trino
        trino_success = insert_into_trino(all_values_list)
        
        elapsed_time = time.time() - start_time
        logger.info(f"Task completed in {elapsed_time:.2f} seconds. Processed {len(company_data)} tickers, saved {len(all_values_list)} records.")
        
    except Exception as e:
        logger.error(f"An error occurred in the main process: {e}", exc_info=True)
        raise

# Define the task
fetch_realtime_prices_task = PythonOperator(
    task_id='fetch_latest_stock_prices_with_accurate_timestamps',
    python_callable=fetch_realtime_stock_prices,
    dag=dag,
)

# Set task order (ready for adding more tasks in the future)
fetch_realtime_prices_task
