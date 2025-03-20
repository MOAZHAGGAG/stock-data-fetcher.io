from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import psycopg2
import logging
import time
import os
import pytz

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Get Airflow DAGs folder path for certificates
AIRFLOW_DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))

# Use absolute paths for SSL certificate files
POSTGRES_SSL_ROOT_CERT = os.path.join(AIRFLOW_DAG_FOLDER, "server-ca.pem")
POSTGRES_SSL_CERT = os.path.join(AIRFLOW_DAG_FOLDER, "client-cert.pem")
POSTGRES_SSL_KEY = os.path.join(AIRFLOW_DAG_FOLDER, "client-key.pem")

# Database connection parameters
db_params = {
    'host': '34.35.40.167',
    'port': '5432',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'password',
    'sslmode': 'verify-ca',
    'sslrootcert': POSTGRES_SSL_ROOT_CERT,
    'sslcert': POSTGRES_SSL_CERT,
    'sslkey': POSTGRES_SSL_KEY
}

# Updated table references
COMPANY_INFO_TABLE = os.getenv("COMPANY_INFO_TABLE", "investments.company_info")
STOCK_PRICES_TABLE = os.getenv("STOCK_PRICES_TABLE", "investments.daily_stock_prices")

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
    'daily_stock_prices_from_postgres_to_postgres_final',
    default_args=default_args,
    description='Fetch company info from PostgreSQL, get stock prices from yfinance, and save to PostgreSQL',
    schedule_interval='0 14 * * 0-4',  # Run once a day at 4:00 pm, Sunday-Thursday
    catchup=False,
)

def get_postgres_connection():
    """Create and return a PostgreSQL connection with SSL."""
    # Verify certificate files exist before attempting connection
    cert_files = {
        "SSL Root Cert": POSTGRES_SSL_ROOT_CERT,
        "SSL Client Cert": POSTGRES_SSL_CERT,
        "SSL Key": POSTGRES_SSL_KEY
    }
    
    for name, path in cert_files.items():
        if not os.path.exists(path):
            raise FileNotFoundError(f"{name} file not found at {path}")
    
    return psycopg2.connect(**db_params)

def get_company_data_from_postgres():
    """Fetch the list of tickers and company codes from the company_info table in PostgreSQL."""
    conn = None
    cursor = None
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT ticker, company_code FROM {COMPANY_INFO_TABLE}")
        company_data = {row[0]: row[1] for row in cursor.fetchall()}
        logger.info(f"Fetched {len(company_data)} companies from PostgreSQL.")
        return company_data
    except Exception as e:
        logger.error(f"Error fetching company data from PostgreSQL: {e}", exc_info=True)
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
            stock = yf.Ticker(ticker)
            quote_info = stock.info
            
            if 'regularMarketPrice' in quote_info and quote_info['regularMarketPrice'] is not None:
                current_price = round(quote_info['regularMarketPrice'], 2)
                
                if 'regularMarketTime' in quote_info and quote_info['regularMarketTime'] is not None:
                    utc_time = datetime.fromtimestamp(quote_info['regularMarketTime'], pytz.UTC)
                    egypt_time = utc_time.astimezone(egypt_tz)
                    trade_date = egypt_time.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    egypt_time = datetime.now(egypt_tz)
                    trade_date = egypt_time.strftime("%Y-%m-%d %H:%M:%S")
                
                current_datetime = datetime.now(egypt_tz).strftime("%Y-%m-%d %H:%M:%S")
                
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
    """Process a batch of tickers and return the data list."""
    data_list = []
    errors = []

    for ticker, company_code in tickers_with_codes.items():
        trade_date, price, ticker_symbol, company_code, current_datetime, error = fetch_realtime_stock_data(ticker, company_code)
        
        if error:
            errors.append(f"{ticker}: {error}")
        else:
            data_list.append((current_datetime, trade_date, company_code, ticker_symbol, price))
            logger.info(f"Fetched {ticker_symbol} - {price} at {trade_date}")
        
        time.sleep(API_RATE_LIMIT_DELAY)

    if errors:
        logger.warning(f"Errors occurred while processing tickers: {', '.join(errors)}")
        
    return data_list

def insert_into_postgres(data_list):
    """Insert data into PostgreSQL table."""
    if not data_list:
        logger.warning("No data to insert into PostgreSQL.")
        return False

    conn = None
    cursor = None
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        
        insert_query = f"""
            INSERT INTO {STOCK_PRICES_TABLE} (fetching_date, trade_date, company_code, ticker, price)
            VALUES (%s, %s, %s, %s, %s)
        """
        
        cursor.executemany(insert_query, data_list)
        conn.commit()
        
        logger.info(f"Data saved to PostgreSQL, {len(data_list)} rows")
        return True
    except Exception as e:
        logger.error(f"Error inserting data into PostgreSQL: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def process_tickers_in_batches(company_data, batch_size=BATCH_SIZE):
    """Process tickers in batches to reduce memory usage and improve reliability."""
    all_data_list = []
    items = list(company_data.items())
    
    for i in range(0, len(items), batch_size):
        batch_items = items[i:i+batch_size]
        batch_dict = dict(batch_items)
        logger.info(f"Processing batch {i//batch_size + 1}/{(len(items) + batch_size - 1)//batch_size}")
        
        data_list = process_ticker_batch(batch_dict)
        all_data_list.extend(data_list)
    
    return all_data_list

def fetch_realtime_stock_prices():
    """Fetch company info from PostgreSQL, get stock prices from yfinance, and save to PostgreSQL."""
    start_time = time.time()
    logger.info("Starting stock price fetch task.")
    
    company_data = get_company_data_from_postgres()
    if not company_data:
        logger.warning("No company data found in PostgreSQL.")
        return
    
    try:
        all_data_list = process_tickers_in_batches(company_data)
        postgres_success = insert_into_postgres(all_data_list)
        
        elapsed_time = time.time() - start_time
        logger.info(f"Task completed in {elapsed_time:.2f} seconds. Processed {len(company_data)} tickers.")
        
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise

# Define the task
fetch_realtime_prices_task = PythonOperator(
    task_id='fetch_latest_stock_prices_with_accurate_timestamps',
    python_callable=fetch_realtime_stock_prices,
    dag=dag,
)

# Set task order
fetch_realtime_prices_task
