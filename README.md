# stock-data-fetcher.io
fetching stock data and write them to iceberg table 


# Stock Prices Data Pipeline Documentation

This document provides a detailed explanation of the **Stock Prices Data Pipeline** implemented using **Apache Airflow**, **Trino**, and **Iceberg**. The pipeline fetches stock prices for a list of companies, stores the data in an **Iceberg table**, and appends the data to a **CSV file** for backup.

---

## Table of Contents
1. [Overview](#overview)
2. [Schema and Table Definitions](#schema-and-table-definitions)
3. [DAG Implementation](#dag-implementation)
4. [Pipeline Workflow](#pipeline-workflow)
5. [Dependencies](#dependencies)
6. [Logging and Error Handling](#logging-and-error-handling)
7. [CSV Backup](#csv-backup)
8. [Conclusion](#conclusion)

---

## Overview

The pipeline is designed to:
1. Fetch stock prices for a predefined list of companies using the **yfinance** library.
2. Store the fetched data in an **Iceberg table** using **Trino**.
3. Append the fetched data to a **CSV file** for backup purposes.

The pipeline is scheduled to run every **15 minutes** using **Apache Airflow**.

---

## Schema and Table Definitions

### Schema
The schema `iceberg.stock_db` is created to organize the tables for stock prices and company information.

```sql
CREATE SCHEMA iceberg.stock_db;
```

### Tables
1. **`stock_prices` Table**:
   - Stores the stock prices with the following columns:
     - `record_date`: Timestamp of the record.
     - `ticker`: Stock ticker symbol.
     - `price`: Stock price at the given timestamp.

   ```sql
   CREATE TABLE iceberg.stock_db.stock_prices (
       record_date TIMESTAMP,
       ticker VARCHAR,
       price DECIMAL(10,2)
   );
   ```

2. **`company_info` Table**:
   - Stores company information with the following columns:
     - `company_code`: Unique code for the company.
     - `company_name`: Name of the company.
     - `ticker`: Stock ticker symbol.

   ```sql
   CREATE TABLE IF NOT EXISTS iceberg.stock_db.company_info (
       company_code VARCHAR,
       company_name VARCHAR,
       ticker VARCHAR
   );
   ```

### Sample Data for `company_info`
The `company_info` table is pre-populated with sample data for 24 companies.

```sql
INSERT INTO iceberg.stock_db.company_info (company_code, company_name, ticker) VALUES
    ('RMDA', 'Tenth Of Ramadan Pharmaceutical', 'EGS381B1C015'),
    ('MPCI', 'Memphis Pharmaceuticals', 'EGS38351C010'),
    ('RACC', 'Raya Contact Center', 'EGS74191C015'),
    ('MFPC', 'Misr Fertilizers Production Co. (MOPCO)', 'EGS39061C014'),
    ('ORWE', 'Oriental Weavers', 'EGS33041C012'),
    ('TALM', 'Taaleem Management Services', 'EGS597R1C017'),
    ('OLFI', 'Obour Land for Food Industries', 'EGS30AL1C012'),
    ('CSAG', 'Canal Shipping Agencies', 'EGS44031C010'),
    ('ORAS', 'Orascom Construction PLC', 'EGS95001C011'),
    ('MASR', 'Madinet Nasr for Housing & Development', 'EGS65571C019'),
    ('KZPC', 'Kafr El Zayat Pesticides', 'EGS38411C012'),
    ('SWDY', 'Elsewedy Electric', 'EGS3G0Z1C014'),
    ('ARCC', 'Arabian Cement Co.', 'EGS3C0O1C016'),
    ('KABO', 'El Nasr Clothing & Textiles (KABO)', 'EGS33061C010'),
    ('JUFO', 'Juhayna Food Industries', 'EGS30901C010'),
    ('DSCW', 'Dice Sport & Casual Wear', 'EGS33321C018'),
    ('ISPH', 'Ibnsina Pharma', 'EGS512O1C012'),
    ('ETRS', 'Egyptian Transport (Egytrans)', 'EGS42051C010'),
    ('SKPC', 'Sidi Kerir Petrochemicals', 'EGS380S1C017'),
    ('EFID', 'Edita Food Industries', 'EGS305I1C011'),
    ('EGAL', 'Egypt Aluminum', 'EGS3E181C010'),
    ('ABUK', 'Abu Qir Fertilizers', 'EGS38191C010'),
    ('POUL', 'Cairo Poultry', 'EGS02051C018'),
    ('SAUD', 'MSCI EUROPE DIVERSIFIED GRTR US/ Baraka bank', 'EGS60101C010');
```

---

## DAG Implementation

The DAG (`stock_dag.py`) is implemented using **Apache Airflow**. It consists of a single task that fetches stock prices and stores them in the Iceberg table and a CSV file.

### Key Components

1. **DAG Definition**:
   - The DAG is named `stock_prices_iceberg_v2`.
   - It runs every **15 minutes** (`schedule_interval=timedelta(minutes=15)`).
   - The start date is set to **October 1, 2023**.

   ```python
   dag = DAG(
       'stock_prices_iceberg_v2',
       default_args=default_args,
       description='Fetch stock prices and store in Iceberg using Trino',
       schedule_interval=timedelta(minutes=15),
       catchup=False,
   )
   ```

2. **Task Definition**:
   - The task `fetch_stock_prices` is implemented using the `PythonOperator`.
   - It calls the `fetch_stock_prices` function to fetch and store stock prices.

   ```python
   fetch_prices_task = PythonOperator(
       task_id='fetch_stock_prices',
       python_callable=fetch_stock_prices,
       dag=dag,
   )
   ```

3. **Fetching Tickers**:
   - The `get_tickers_from_trino` function fetches the list of tickers from the `company_info` table in Iceberg.

   ```python
   def get_tickers_from_trino():
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
       return tickers
   ```

4. **Fetching Stock Prices**:
   - The `fetch_stock_prices` function uses the **yfinance** library to fetch stock prices for each ticker.
   - The fetched data is inserted into the `stock_prices` table in Iceberg.

   ```python
   for ticker in tickers:
       stock = yf.Ticker(ticker)
       current_price = round(stock.history(period="1d")["Close"].iloc[-1], 2)
       timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
       values_list.append(f"(TIMESTAMP '{timestamp}', '{ticker}', {current_price})")
   ```

5. **CSV Backup**:
   - The fetched data is also appended to a CSV file (`stock_prices.csv`) for backup.

   ```python
   with open(CSV_FILE, 'a', newline='') as csvfile:
       writer = csv.writer(csvfile)
       if not file_exists:
           writer.writerow(['record_date', 'ticker', 'price'])
       writer.writerows(data_list)
   ```

---

## Pipeline Workflow

1. The DAG starts and executes the `fetch_stock_prices` task.
2. The task fetches the list of tickers from the `company_info` table.
3. For each ticker, it fetches the stock price using **yfinance**.
4. The fetched data is inserted into the `stock_prices` table in Iceberg.
5. The data is also appended to the `stock_prices.csv` file.
6. The task completes, and the DAG waits for the next scheduled run.

---

## Dependencies

The following Python libraries are required:
- **yfinance**: For fetching stock prices.
- **trino**: For interacting with the Trino server.
- **csv**: For writing data to a CSV file.
- **os**: For checking if the CSV file exists.
- **logging**: For logging pipeline activities.

Install the dependencies using pip:

```bash
pip install yfinance trino
```

---

## Logging and Error Handling

- The pipeline uses Python's `logging` module to log activities and errors.
- Errors are logged with detailed traceback information for debugging.

Example log messages:
- `Fetched 24 tickers from company_info`
- `Fetched EGS381B1C015 - 45.67 at 2023-10-01 12:00:00`
- `Error fetching data for EGS381B1C015: ...`

---

## CSV Backup

The fetched stock prices are appended to a CSV file (`stock_prices.csv`) with the following columns:
- `record_date`: Timestamp of the record.
- `ticker`: Stock ticker symbol.
- `price`: Stock price at the given timestamp.

Example CSV content:
```
record_date,ticker,price
2023-10-01 12:00:00,EGS381B1C015,45.67
2023-10-01 12:15:00,EGS381B1C015,46.12
```

---

## Conclusion

This pipeline provides a robust solution for fetching and storing stock prices in an Iceberg table using Trino. It also ensures data backup by appending the fetched data to a CSV file. The use of Apache Airflow allows for easy scheduling and monitoring of the pipeline.
