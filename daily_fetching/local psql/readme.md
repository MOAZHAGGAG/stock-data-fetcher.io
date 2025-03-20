# Airflow DAG Documentation: Daily Stock Prices from Iceberg to PostgreSQL

## Overview
This Airflow DAG (`daily_stock_prices_from_yfinance_iceberg_to_postgres`) is designed to fetch company information from an **Iceberg table** (via Trino), retrieve real-time stock prices using the `yfinance` API, and save the data into a **PostgreSQL** table. The DAG runs daily at 4:00 PM (Sunday to Thursday) and processes stock data in batches to ensure efficiency and reliability.

---

## Key Features
1. **Fetch Company Data**: Retrieves company tickers and codes from an Iceberg table (`stock_db.company_info`) using Trino.
2. **Real-Time Stock Data**: Uses the `yfinance` API to fetch real-time stock prices and timestamps.
3. **Data Validation**: Validates stock data before insertion to ensure data integrity.
4. **Batch Processing**: Processes tickers in configurable batches to optimize memory usage and API rate limits.
5. **Error Handling**: Implements retries and error logging for robust execution.
6. **PostgreSQL Integration**: Saves processed stock data into a PostgreSQL table (`stock_db.daily_stock_prices`).

---

## DAG Configuration

### Schedule
- **Cron Expression**: `0 14 * * 0-4` (Runs at 4:00 PM UTC, Sunday to Thursday).
- **Start Date**: March 1, 2025.
- **Catchup**: Disabled (`catchup=False`).

### Default Arguments
- **Owner**: `airflow`.
- **Retries**: 2.
- **Retry Delay**: 1 minute.
- **Email Notifications**: Enabled for failures.

---

## Environment Variables
The DAG uses environment variables for flexibility and configuration:

| Variable Name               | Default Value                          | Description                                                                 |
|-----------------------------|----------------------------------------|-----------------------------------------------------------------------------|
| `TRINO_HOST`                | `trino`                               | Hostname of the Trino server.                                              |
| `TRINO_PORT`                | `8080`                                | Port of the Trino server.                                                  |
| `TRINO_USER`                | `airflow`                             | Username for Trino connection.                                             |
| `TRINO_CATALOG`             | `iceberg`                             | Catalog name in Trino.                                                     |
| `TRINO_SCHEMA`              | `stock_db`                            | Schema name in Trino.                                                      |
| `COMPANY_TABLE`             | `company_info`                        | Iceberg table containing company tickers and codes.                        |
| `POSTGRES_HOST`             | `postgres`                            | Hostname of the PostgreSQL server.                                         |
| `POSTGRES_PORT`             | `5432`                                | Port of the PostgreSQL server.                                             |
| `POSTGRES_DB`               | `admin`                               | PostgreSQL database name.                                                  |
| `POSTGRES_USER`             | `admin`                               | Username for PostgreSQL connection.                                        |
| `POSTGRES_PASSWORD`         | `admin`                               | Password for PostgreSQL connection.                                         |
| `POSTGRES_TABLE`            | `stock_db.daily_stock_prices`         | PostgreSQL table to store fetched stock prices.                             |
| `BATCH_SIZE`                | `12`                                  | Number of tickers to process in each batch.                                 |
| `MAX_RETRIES`               | `3`                                   | Maximum retries for fetching stock data from `yfinance`.                    |
| `RETRY_DELAY`               | `10`                                  | Delay (in seconds) between retries for `yfinance` API calls.                |
| `API_RATE_LIMIT_DELAY`      | `2`                                   | Delay (in seconds) between API calls to respect rate limits.                |

---

## Workflow

### Tasks
1. **Fetch Latest Stock Prices** (`fetch_latest_stock_prices_with_accurate_timestamps`):
   - Fetches company data from Iceberg via Trino.
   - Retrieves real-time stock prices and timestamps using `yfinance`.
   - Validates and processes data in batches.
   - Inserts validated data into the PostgreSQL `daily_stock_prices` table.

---

## Functions

### `get_trino_connection()`
- **Purpose**: Establishes a connection to Trino for querying Iceberg tables.
- **Code**:
  ```python
  def get_trino_connection():
      """Create and return a Trino connection."""
      return trino.dbapi.connect(
          host=TRINO_HOST,
          port=TRINO_PORT,
          user=TRINO_USER,
          catalog=TRINO_CATALOG,
          schema=TRINO_SCHEMA
      )
  ```

### `get_postgres_connection()`
- **Purpose**: Establishes a connection to PostgreSQL for saving stock data.
- **Code**:
  ```python
  def get_postgres_connection():
      """Create and return a PostgreSQL connection."""
      return psycopg2.connect(
          host=POSTGRES_HOST,
          port=POSTGRES_PORT,
          dbname=POSTGRES_DB,
          user=POSTGRES_USER,
          password=POSTGRES_PASSWORD
      )
  ```

### `get_company_data_from_trino()`
- **Purpose**: Fetches company tickers and codes from the Iceberg `company_info` table.
- **Code**:
  ```python
  def get_company_data_from_trino():
      """Fetch the list of tickers and company codes from the company_info table in Iceberg."""
      conn = None
      cursor = None
      try:
          conn = get_trino_connection()
          cursor = conn.cursor()
          cursor.execute(f"SELECT ticker, company_code FROM {COMPANY_TABLE}")
          company_data = {row[0]: row[1] for row in cursor.fetchall()}
          logger.info(f"Fetched {len(company_data)} companies from Iceberg table {COMPANY_TABLE}")
          return company_data
      except Exception as e:
          logger.error(f"Error fetching company data from Trino: {e}", exc_info=True)
          return {}
      finally:
          if cursor:
              cursor.close()
          if conn:
              conn.close()
  ```

### `validate_stock_data()`
- **Purpose**: Validates stock data before insertion.
- **Code**:
  ```python
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
  ```

### `fetch_realtime_stock_data()`
- **Purpose**: Fetches real-time stock data using `yfinance`.
- **Code**:
  ```python
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
  ```

### `process_ticker_batch()`
- **Purpose**: Processes a batch of tickers and fetches their stock data.
- **Code**:
  ```python
  def process_ticker_batch(tickers_with_codes):
      """Process a batch of tickers and return the data list."""
      data_list = []
      errors = []

      for ticker, company_code in tickers_with_codes.items():
          logger.info(f"Fetching latest price for {ticker} (company code: {company_code})...")
          trade_date, price, ticker_symbol, company_code, current_datetime, error = fetch_realtime_stock_data(ticker, company_code)
          
          if error:
              errors.append(f"{ticker}: {error}")
          else:
              data_list.append((current_datetime, trade_date, company_code, ticker_symbol, price))
              logger.info(f"Fetched {ticker_symbol} - {price} at {trade_date}, company code: {company_code}, current datetime: {current_datetime}")
          
          time.sleep(API_RATE_LIMIT_DELAY)

      if errors:
          logger.warning(f"Errors occurred while processing tickers: {', '.join(errors)}")
          
      return data_list
  ```

### `insert_into_postgres()`
- **Purpose**: Inserts processed stock data into the `daily_stock_prices` table.
- **Code**:
  ```python
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
          
          cursor.execute(f"""
              CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE} (
                  id SERIAL PRIMARY KEY,
                  fetching_date TIMESTAMP NOT NULL,
                  trade_date TIMESTAMP NOT NULL,
                  company_code VARCHAR(50) NOT NULL,
                  ticker VARCHAR(20) NOT NULL,
                  price NUMERIC(10, 2) NOT NULL
              )
          """)
          
          insert_query = f"""
              INSERT INTO {POSTGRES_TABLE} (fetching_date, trade_date, company_code, ticker, price)
              VALUES (%s, %s, %s, %s, %s)
          """
          
          cursor.executemany(insert_query, data_list)
          conn.commit()
          
          logger.info(f"Data saved to PostgreSQL table '{POSTGRES_TABLE}', {len(data_list)} rows")
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
  ```

### `process_tickers_in_batches()`
- **Purpose**: Processes all tickers in configurable batches.
- **Code**:
  ```python
  def process_tickers_in_batches(company_data, batch_size=BATCH_SIZE):
      """Process tickers in batches to reduce memory usage and improve reliability."""
      all_data_list = []
      items = list(company_data.items())
      
      for i in range(0, len(items), batch_size):
          batch_items = items[i:i+batch_size]
          batch_dict = dict(batch_items)
          logger.info(f"Processing batch {i//batch_size + 1}/{(len(items) + batch_size - 1)//batch_size} with {len(batch_dict)} tickers")
          
          data_list = process_ticker_batch(batch_dict)
          all_data_list.extend(data_list)
      
      return all_data_list
  ```

### `fetch_realtime_stock_prices()`
- **Purpose**: Main function to orchestrate the workflow.
- **Code**:
  ```python
  def fetch_realtime_stock_prices():
      """Fetch company info from Iceberg, get stock prices from yfinance, and save to PostgreSQL."""
      start_time = time.time()
      logger.info("Starting latest stock price fetch task with accurate timestamps")
      
      company_data = get_company_data_from_trino()
      if not company_data:
          logger.warning("No company data found in Iceberg, skipping stock price fetching.")
          return
      
      logger.info(f"Processing {len(company_data)} companies for latest data with accurate timestamps")
      
      try:
          all_data_list = process_tickers_in_batches(company_data)
          postgres_success = insert_into_postgres(all_data_list)
          
          elapsed_time = time.time() - start_time
          logger.info(f"Task completed in {elapsed_time:.2f} seconds. Processed {len(company_data)} tickers, saved {len(all_data_list)} records.")
          
      except Exception as e:
          logger.error(f"An error occurred in the main process: {e}", exc_info=True)
          raise
  ```

---

## Database Schema

### Iceberg Table: `stock_db.company_info`
| Column Name   | Data Type | Description                |
|---------------|-----------|----------------------------|
| `ticker`      | `text`    | Stock ticker symbol.       |
| `company_code`| `text`    | Unique company identifier. |

### PostgreSQL Table: `stock_db.daily_stock_prices`
| Column Name      | Data Type | Description                          |
|------------------|-----------|--------------------------------------|
| `id`             | `SERIAL`  | Primary key.                         |
| `fetching_date`  | `TIMESTAMP` | Timestamp when data was fetched.   |
| `trade_date`     | `TIMESTAMP` | Timestamp of the stock trade.      |
| `company_code`   | `VARCHAR(50)` | Unique company identifier.         |
| `ticker`         | `VARCHAR(20)` | Stock ticker symbol.               |
| `price`          | `NUMERIC(10, 2)` | Stock price at the trade timestamp.|

---

## Error Handling
- **API Failures**: Retries up to `MAX_RETRIES` with a delay of `RETRY_DELAY` seconds.
- **Data Validation**: Skips invalid data and logs warnings.
- **Database Errors**: Rolls back transactions on insertion failures.

---

## Logging
- **Log Level**: `INFO`.
- **Format**: `%(asctime)s - %(levelname)s - %(message)s`.
- **Details**:
  - Logs the number of companies fetched.
  - Logs the status of each ticker processed.
  - Logs errors and warnings during execution.

---

## Example Log Output
```
2025-03-01 16:00:00 - INFO - Starting latest stock price fetch task with accurate timestamps.
2025-03-01 16:00:01 - INFO - Fetched 100 companies from Iceberg table company_info.
2025-03-01 16:00:05 - INFO - Processing batch 1/9 with 12 tickers.
2025-03-01 16:00:10 - INFO - Fetched AAPL - 150.25 at 2025-03-01 15:59:59, company code: 123, current datetime: 2025-03-01 16:00:10.
2025-03-01 16:00:15 - WARNING - No price available for XYZ.
2025-03-01 16:05:00 - INFO - Task completed in 300.12 seconds. Processed 100 tickers, saved 98 records.
```

---

## Dependencies
- **Python Packages**:
  - `airflow`
  - `yfinance`
  - `trino`
  - `psycopg2`
  - `pytz`
- **External Services**:
  - Trino server (for Iceberg tables).
  - PostgreSQL database.
  - `yfinance` API.

---

## Notes
- Ensure the Trino and PostgreSQL servers are accessible from the Airflow environment.
- Adjust `BATCH_SIZE`, `MAX_RETRIES`, and `API_RATE_LIMIT_DELAY` as needed to optimize performance and API usage.
- Test the DAG in a development environment before deploying to production.
