from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import trino
import logging
import time
import csv
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Trino connection settings
TRINO_HOST = "trino"
TRINO_PORT = 8080
TRINO_CATALOG = "iceberg"
TRINO_SCHEMA = "stock_db"
COMPANY_TABLE = "company_info"
TRINO_TABLE = "stock_prices"

# CSV file path
CSV_FILE = "stock_prices.csv"

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'stock_prices_iceberg_v2',
    default_args=default_args,
    description='Fetch stock prices and store in Iceberg using Trino',
    schedule_interval=timedelta(minutes=15),
    catchup=False,
)

def get_tickers_from_trino():
    """
    Fetch the list of tickers from the company_info table in Iceberg.
    """
    try:
        conn = trino.dbapi.connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user="airflow",
            catalog=TRINO_CATALOG,
            schema=TRINO_SCHEMA
        )
        cursor = conn.cursor()
        cursor.execute(f"SELECT ticker FROM {COMPANY_TABLE}")
        tickers = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        logger.info(f"Fetched {len(tickers)} tickers from {COMPANY_TABLE}")
        return tickers
    except Exception as e:
        logger.error(f"Error fetching tickers: {e}", exc_info=True)
        return []

def fetch_stock_prices():
    """
    Fetch stock prices using yfinance and insert them into Iceberg (Trino) and a CSV file.
    """
    tickers = get_tickers_from_trino()
    if not tickers:
        logger.warning("No tickers found, skipping stock price fetching.")
        return

    try:
        conn = trino.dbapi.connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user="airflow",
            catalog=TRINO_CATALOG,
            schema=TRINO_SCHEMA
        )
        cursor = conn.cursor()

        values_list = []
        data_list = []

        for ticker in tickers:
            logger.info(f"Fetching price for {ticker}...")
            stock = yf.Ticker(ticker)
            try:
                current_price = round(stock.history(period="1d")["Close"].iloc[-1], 2)
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                values_list.append(f"(TIMESTAMP '{timestamp}', '{ticker}', {current_price})")
                data_list.append((timestamp, ticker, current_price))
                logger.info(f"Fetched {ticker} - {current_price} at {timestamp}")
            except Exception as e:
                logger.error(f"Error fetching data for {ticker}: {e}")

            time.sleep(5)  # Avoid hitting rate limits

        if values_list:
            query = f"INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE} VALUES {', '.join(values_list)}"
            cursor.execute(query)
            logger.info(f"Data saved to Iceberg table '{TRINO_SCHEMA}.{TRINO_TABLE}'")
        else:
            logger.warning("No data to insert into Iceberg.")

        cursor.close()
        conn.close()

        # Save data to CSV file
        if data_list:
            file_exists = os.path.isfile(CSV_FILE)
            with open(CSV_FILE, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                if not file_exists:
                    writer.writerow(['record_date', 'ticker', 'price'])
                writer.writerows(data_list)
            logger.info(f"Data appended to CSV file '{CSV_FILE}'")
        else:
            logger.warning("No data to write to CSV.")

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise

# Define the task
fetch_prices_task = PythonOperator(
    task_id='fetch_stock_prices',
    python_callable=fetch_stock_prices,
    dag=dag,
)

# Set task order
fetch_prices_task
