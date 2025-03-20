# Airflow DAG Documentation: Daily Stock Prices from PostgreSQL to PostgreSQL

## Overview
This Airflow DAG (`daily_stock_prices_from_postgres_to_postgres_final`) is designed to fetch company information from a PostgreSQL database, retrieve real-time stock prices using the `yfinance` API, and save the stock price data back into another PostgreSQL table. The DAG runs daily at 4:00 PM (Sunday to Thursday) and processes stock data in batches to ensure efficiency and reliability.

---

## Key Features
1. **Fetch Company Data**: Retrieves company tickers and codes from a PostgreSQL table (`investments.company_info`).
2. **Real-Time Stock Data**: Uses the `yfinance` API to fetch real-time stock prices and timestamps.
3. **Data Validation**: Validates stock data before insertion to ensure data integrity.
4. **Batch Processing**: Processes tickers in configurable batches to optimize memory usage and API rate limits.
5. **Error Handling**: Implements retries and error logging for robust execution.
6. **PostgreSQL Integration**: Saves processed stock data into a PostgreSQL table (`investments.daily_stock_prices`) with SSL encryption for secure connections.

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
| `COMPANY_INFO_TABLE`         | `investments.company_info`             | PostgreSQL table containing company tickers and codes.                      |
| `STOCK_PRICES_TABLE`         | `investments.daily_stock_prices`       | PostgreSQL table to store fetched stock prices.                             |
| `BATCH_SIZE`                 | `12`                                   | Number of tickers to process in each batch.                                 |
| `MAX_RETRIES`                | `3`                                    | Maximum retries for fetching stock data from `yfinance`.                    |
| `RETRY_DELAY`                | `10`                                   | Delay (in seconds) between retries for `yfinance` API calls.                |
| `API_RATE_LIMIT_DELAY`       | `2`                                    | Delay (in seconds) between API calls to respect rate limits.                |

---

## Workflow

### Tasks
1. **Fetch Latest Stock Prices** (`fetch_latest_stock_prices_with_accurate_timestamps`):
   - Fetches company data from PostgreSQL.
   - Retrieves real-time stock prices and timestamps using `yfinance`.
   - Validates and processes data in batches.
   - Inserts validated data into the PostgreSQL `daily_stock_prices` table.

---

## Functions

### `get_postgres_connection()`
- **Purpose**: Establishes a secure PostgreSQL connection using SSL certificates.
- **Certificates**:
  - `server-ca.pem`: Root certificate.
  - `client-cert.pem`: Client certificate.
  - `client-key.pem`: Client private key.
- **Error Handling**: Raises `FileNotFoundError` if certificate files are missing.

### `get_company_data_from_postgres()`
- **Purpose**: Fetches company tickers and codes from the `company_info` table.
- **Returns**: A dictionary mapping tickers to company codes.

### `validate_stock_data()`
- **Purpose**: Validates stock data before insertion.
- **Checks**:
  - Valid ticker symbol.
  - Positive and non-null price.
  - Valid trade date and current datetime.
  - Non-empty company code.

### `fetch_realtime_stock_data()`
- **Purpose**: Fetches real-time stock data using `yfinance`.
- **Retries**: Up to `MAX_RETRIES` with a delay of `RETRY_DELAY` seconds.
- **Returns**: Trade date, price, ticker, company code, current datetime, and error message (if any).

### `process_ticker_batch()`
- **Purpose**: Processes a batch of tickers and fetches their stock data.
- **Returns**: A list of validated stock data tuples.

### `insert_into_postgres()`
- **Purpose**: Inserts processed stock data into the `daily_stock_prices` table.
- **Error Handling**: Rolls back the transaction on failure.

### `process_tickers_in_batches()`
- **Purpose**: Processes all tickers in configurable batches.
- **Returns**: A combined list of stock data for all batches.

### `fetch_realtime_stock_prices()`
- **Purpose**: Main function to orchestrate the workflow.
- **Steps**:
  1. Fetches company data from PostgreSQL.
  2. Processes tickers in batches.
  3. Inserts validated data into PostgreSQL.
  4. Logs the total execution time.

---

## Database Schema

### `investments.company_info`
| Column Name   | Data Type | Description                |
|---------------|-----------|----------------------------|
| `ticker`      | `text`    | Stock ticker symbol.       |
| `company_code`| `text`    | Unique company identifier. |

### `investments.daily_stock_prices`
| Column Name      | Data Type | Description                          |
|------------------|-----------|--------------------------------------|
| `fetching_date`  | `timestamp` | Timestamp when data was fetched.   |
| `trade_date`     | `timestamp` | Timestamp of the stock trade.      |
| `company_code`   | `text`    | Unique company identifier.           |
| `ticker`         | `text`    | Stock ticker symbol.                 |
| `price`          | `numeric` | Stock price at the trade timestamp.  |

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
2025-03-01 16:00:00 - INFO - Starting stock price fetch task.
2025-03-01 16:00:01 - INFO - Fetched 100 companies from PostgreSQL.
2025-03-01 16:00:05 - INFO - Processing batch 1/9
2025-03-01 16:00:10 - INFO - Fetched AAPL - 150.25 at 2025-03-01 15:59:59
2025-03-01 16:00:15 - WARNING - No price available for XYZ
2025-03-01 16:05:00 - INFO - Task completed in 300.12 seconds. Processed 100 tickers.
```

---

## SSL Configuration
- **Certificates**:
  - `server-ca.pem`: Root certificate for verifying the server.
  - `client-cert.pem`: Client certificate for authentication.
  - `client-key.pem`: Client private key for authentication.
- **SSL Mode**: `verify-ca` (ensures the server certificate is validated).

---

## Dependencies
- **Python Packages**:
  - `airflow`
  - `yfinance`
  - `psycopg2`
  - `pytz`
- **External Services**:
  - PostgreSQL database.
  - `yfinance` API.

---

## Notes
- Ensure the SSL certificate files are placed in the Airflow DAGs folder.
- Adjust `BATCH_SIZE`, `MAX_RETRIES`, and `API_RATE_LIMIT_DELAY` as needed to optimize performance and API usage.
- Test the DAG in a development environment before deploying to production.
