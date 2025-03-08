### Documentation for the Airflow DAG: `daily_stock_prices_from_yfinance_v2`

This document provides an overview of the Airflow DAG (`daily_stock_prices_from_yfinance_v2`) designed to fetch real-time stock prices from Yahoo Finance (`yfinance`) and store them in a Trino/Iceberg table (`daily_stock_prices`). The DAG is scheduled to run once a day at 5:00 PM (Cairo time) from Sunday to Thursday.

---

### **Purpose**
The DAG performs the following tasks:
1. Fetches a list of stock tickers and company codes from a Trino/Iceberg table (`company_info`).
2. Retrieves real-time stock prices and timestamps for each ticker using the `yfinance` API.
3. Validates the fetched data.
4. Inserts the validated data into the `daily_stock_prices` table in Trino/Iceberg.

---

### **Table Schema**
The `daily_stock_prices` table is created with the following schema:

```sql
CREATE TABLE IF NOT EXISTS iceberg.stock_db.daily_stock_prices (
    fetching_date TIMESTAMP COMMENT 'Date when the price was fetched',
    trade_date TIMESTAMP COMMENT 'Date when the price changed',
    company_code VARCHAR COMMENT 'The company code identifier',
    ticker VARCHAR COMMENT 'The stock ticker symbol',
    price DECIMAL(10, 2) COMMENT 'The stock price value'
);
```

#### **Columns**
1. **`fetching_date`**: The timestamp when the stock price was fetched.
2. **`trade_date`**: The timestamp when the stock price was last updated (as reported by Yahoo Finance).
3. **`company_code`**: A unique identifier for the company.
4. **`ticker`**: The stock ticker symbol (e.g., AAPL for Apple).
5. **`price`**: The stock price at the time of fetching.

---

### **DAG Configuration**
#### **Schedule**
The DAG runs daily at 5:00 PM (Cairo time) from Sunday to Thursday, as defined by the cron expression:
```python
schedule_interval='0 15 * * 0-4'
```

#### **Default Arguments**
```python
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': True,
    'email_on_retry': False,
}
```

#### **Environment Variables**
The DAG uses the following environment variables for configuration:
- **Trino Connection**:
  - `TRINO_HOST`: Trino server host (default: `trino`).
  - `TRINO_PORT`: Trino server port (default: `8080`).
  - `TRINO_USER`: Trino username (default: `airflow`).
  - `TRINO_CATALOG`: Trino catalog (default: `iceberg`).
  - `TRINO_SCHEMA`: Trino schema (default: `stock_db`).
  - `COMPANY_TABLE`: Table containing company info (default: `company_info`).
  - `TRINO_TABLE`: Table to store stock prices (default: `daily_stock_prices`).

- **Batch Processing**:
  - `BATCH_SIZE`: Number of tickers to process in a single batch (default: `12`).
  - `MAX_RETRIES`: Maximum number of retries for fetching stock data (default: `3`).
  - `RETRY_DELAY`: Delay (in seconds) between retries (default: `10`).
  - `API_RATE_LIMIT_DELAY`: Delay (in seconds) between API calls to respect rate limits (default: `2`).

---

### **Workflow**
The DAG consists of a single task (`fetch_realtime_prices_task`) that performs the following steps:

1. **Fetch Company Data**:
   - Connects to Trino and retrieves a list of tickers and company codes from the `company_info` table.

2. **Fetch Stock Prices**:
   - For each ticker, fetches real-time stock prices and timestamps using the `yfinance` API.
   - Validates the fetched data to ensure it meets the required criteria.

3. **Batch Processing**:
   - Processes tickers in batches to reduce memory usage and improve reliability.

4. **Insert Data into Trino**:
   - Inserts the validated stock price data into the `daily_stock_prices` table in Trino/Iceberg.

---

### **Key Functions**
1. **`get_trino_connection()`**:
   - Establishes a connection to the Trino server.

2. **`get_company_data_from_trino()`**:
   - Fetches the list of tickers and company codes from the `company_info` table.

3. **`fetch_realtime_stock_data()`**:
   - Fetches real-time stock data (price and timestamp) for a given ticker using the `yfinance` API.
   - Converts timestamps to Cairo time (Africa/Cairo).

4. **`validate_stock_data()`**:
   - Validates the fetched stock data to ensure it is complete and accurate.

5. **`process_ticker_batch()`**:
   - Processes a batch of tickers and returns a list of values to be inserted into Trino.

6. **`insert_into_trino()`**:
   - Inserts the processed stock price data into the `daily_stock_prices` table.

7. **`process_tickers_in_batches()`**:
   - Processes all tickers in batches to improve efficiency and reliability.

8. **`fetch_realtime_stock_prices()`**:
   - The main function that orchestrates the fetching, validation, and insertion of stock price data.

---

### **Error Handling**
- The DAG includes retry logic for fetching stock data, with a configurable number of retries (`MAX_RETRIES`) and delay between retries (`RETRY_DELAY`).
- Errors during data fetching or insertion are logged and reported.

---

### **Logging**
- The DAG uses Python's `logging` module to log information, warnings, and errors.
- Logs include details such as:
  - The number of companies processed.
  - The number of records inserted into Trino.
  - Errors encountered during data fetching or insertion.

---

### **Example Log Output**
```
2025-03-01 17:00:00,123 - INFO - Starting latest stock price fetch task with accurate timestamps
2025-03-01 17:00:01,456 - INFO - Fetched 100 companies from company_info
2025-03-01 17:00:02,789 - INFO - Processing batch 1/9 with 12 tickers
2025-03-01 17:00:03,012 - INFO - Retrieved quote data for AAPL(code: 1): price=150.25 at 2025-03-01 16:59:59, current datetime=2025-03-01 17:00:02
2025-03-01 17:00:04,345 - INFO - Data saved to Iceberg table 'stock_db.daily_stock_prices', 12 rows
2025-03-01 17:00:05,678 - INFO - Task completed in 5.55 seconds. Processed 100 tickers, saved 100 records.
```

---

### **Dependencies**
- **Python Libraries**:
  - `airflow`
  - `yfinance`
  - `trino`
  - `pytz`
  - `pendulum`

- **External Services**:
  - Trino server (for storing and retrieving data).
  - Yahoo Finance API (for fetching stock prices).

---


This concludes the documentation for the `daily_stock_prices_from_yfinance_v2` DAG.
