# Updated Stock Prices Data Pipeline Documentation

This document provides a detailed explanation of the **updated Stock Prices Data Pipeline**. The pipeline now fetches stock prices with **accurate timestamps** from **yfinance**, processes the data in **batches**, and stores it in an **Iceberg table** using **Trino**. The pipeline is scheduled to run every **5 minutes** during the **Egyptian Stock Market working hours** (10 AM to 2:59 PM, Sunday to Thursday).

---

## Table of Contents
1. [Overview](#overview)
2. [Key Updates](#key-updates)
3. [Pipeline Workflow](#pipeline-workflow)
4. [DAG Configuration](#dag-configuration)
5. [Batch Processing](#batch-processing)
6. [Time Zone Handling](#time-zone-handling)
7. [Data Validation](#data-validation)
8. [Error Handling and Retries](#error-handling-and-retries)
9. [Conclusion](#conclusion)

---

## Overview

The updated pipeline now:
1. Fetches stock prices with **accurate timestamps** from **yfinance**.
2. Processes tickers in **batches** to improve reliability and reduce memory usage.
3. Runs every **5 minutes** during the **Egyptian Stock Market working hours** (10 AM to 2:59 PM, Sunday to Thursday).
4. Validates stock data before insertion.
5. Handles errors and retries gracefully.

---

## Key Updates

### 1. **Accurate Timestamps**
   - The pipeline now fetches the **actual timestamp** associated with the stock price from **yfinance**.
   - The timestamp is converted to **Egypt time (Africa/Cairo)** using the `pytz` library.

   ```python
   egypt_tz = pytz.timezone('Africa/Cairo')
   utc_time = datetime.fromtimestamp(quote_info['regularMarketTime'], pytz.UTC)
   egypt_time = utc_time.astimezone(egypt_tz)
   trade_date = egypt_time.strftime("%Y-%m-%d %H:%M:%S")
   ```

### 2. **Batch Processing**
   - Tickers are processed in **batches** to reduce memory usage and improve reliability.
   - The batch size is configurable via the `BATCH_SIZE` environment variable (default: 12).

   ```python
   def process_tickers_in_batches(tickers, batch_size=BATCH_SIZE):
       for i in range(0, len(tickers), batch_size):
           batch = tickers[i:i+batch_size]
           values_list = process_ticker_batch(batch)
   ```

### 3. **Scheduling**
   - The DAG runs every **5 minutes** during the **Egyptian Stock Market working hours** (10 AM to 2:59 PM, Sunday to Thursday).
   - The schedule is defined using a **cron expression**:

   ```python
   schedule_interval='*/5 8-12 * * 0-4'
   ```

   - Explanation:
     - `*/5`: Every 5 minutes.
     - `8-12`: From 10 AM to 2:59 PM (UTC+2).
     - `0-4`: Sunday to Thursday.

### 4. **Data Validation**
   - Stock data is validated before insertion to ensure:
     - The ticker is valid.
     - The price is a positive number.
     - The timestamp is valid.

   ```python
   def validate_stock_data(ticker, price, trade_date):
       if not ticker or not isinstance(ticker, str):
           return False, "Invalid ticker symbol"
       if price is None or not isinstance(price, (int, float)) or price <= 0:
           return False, f"Invalid price for {ticker}: {price}"
       if not trade_date:
           return False, f"Invalid trade date for {ticker}"
       return True, None
   ```

### 5. **Error Handling and Retries**
   - The pipeline retries failed API calls up to `MAX_RETRIES` times (default: 3).
   - A delay (`RETRY_DELAY`) is added between retries (default: 10 seconds).

   ```python
   for attempt in range(max_retries):
       try:
           # Fetch data
       except Exception as e:
           logger.warning(f"Attempt {attempt+1}/{max_retries} failed for {ticker}: {e}")
           if attempt < max_retries - 1:
               time.sleep(retry_delay)
   ```

---

## Pipeline Workflow

1. The DAG starts and executes the `fetch_realtime_stock_prices` task.
2. The task fetches the list of tickers from the `company_info` table in Iceberg.
3. Tickers are processed in **batches**:
   - For each ticker, the pipeline fetches the stock price and its associated timestamp from **yfinance**.
   - The timestamp is converted to **Egypt time**.
   - The data is validated before insertion.
4. Valid data is inserted into the `stock_prices` table in Iceberg.
5. The task logs the results and waits for the next scheduled run.

---

## DAG Configuration

### Environment Variables
The pipeline uses environment variables for configuration:

| Variable               | Default Value | Description                                   |
|------------------------|---------------|-----------------------------------------------|
| `TRINO_HOST`           | `trino`       | Trino server host.                            |
| `TRINO_PORT`           | `8080`        | Trino server port.                            |
| `TRINO_USER`           | `airflow`     | Trino user.                                   |
| `TRINO_CATALOG`        | `iceberg`     | Trino catalog.                                |
| `TRINO_SCHEMA`         | `stock_db`    | Trino schema.                                 |
| `COMPANY_TABLE`        | `company_info`| Table containing company information.         |
| `TRINO_TABLE`          | `stock_prices`| Table to store stock prices.                  |
| `BATCH_SIZE`           | `12`          | Number of tickers to process in a batch.      |
| `MAX_RETRIES`          | `3`           | Maximum number of retries for failed API calls.|
| `RETRY_DELAY`          | `10`          | Delay (in seconds) between retries.           |
| `API_RATE_LIMIT_DELAY` | `2`           | Delay (in seconds) between API calls.         |

### Default DAG Arguments
The DAG is configured with the following default arguments:

```python
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': True,
    'email_on_retry': False,
}
```

---

## Batch Processing

- Tickers are processed in batches to reduce memory usage and improve reliability.
- The batch size is configurable via the `BATCH_SIZE` environment variable.
- Each batch is processed sequentially, with a delay (`API_RATE_LIMIT_DELAY`) between API calls to respect rate limits.

---

## Time Zone Handling

- The pipeline uses the **Africa/Cairo** time zone to ensure timestamps are accurate for the Egyptian Stock Market.
- Timestamps fetched from **yfinance** are converted from UTC to Egypt time using the `pytz` library.

---

## Data Validation

- Stock data is validated before insertion to ensure:
  - The ticker is a non-empty string.
  - The price is a positive number.
  - The timestamp is valid.

---

## Error Handling and Retries

- The pipeline retries failed API calls up to `MAX_RETRIES` times.
- A delay (`RETRY_DELAY`) is added between retries to avoid overwhelming the API.
- Errors are logged for debugging and monitoring.

---

## Conclusion

The updated pipeline provides a robust and efficient solution for fetching and storing stock prices with **accurate timestamps**. The use of **batch processing**, **data validation**, and **error handling** ensures reliability and scalability. The pipeline is optimized for the **Egyptian Stock Market working hours** and can be easily configured using environment variables.
