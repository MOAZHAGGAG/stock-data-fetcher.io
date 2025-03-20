import os
import pandas as pd
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
import trino
import psycopg2
import psycopg2.extras

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Get Airflow DAGs folder path for certificates
AIRFLOW_DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))

# Use absolute paths for SSL certificate files
POSTGRES_SSL_ROOT_CERT = os.path.join(AIRFLOW_DAG_FOLDER, "server-ca.pem")
POSTGRES_SSL_CERT = os.path.join(AIRFLOW_DAG_FOLDER, "client-cert.pem")
POSTGRES_SSL_KEY = os.path.join(AIRFLOW_DAG_FOLDER, "client-key.pem")

# Trino connection settings for output data
TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "airflow")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "stock_db")
TRINO_TABLE = os.getenv("TRINO_TABLE", "daily_stock_prices")

# PostgreSQL connection settings with SSL
DB_PARAMS = {
    'host': '34.35.40.167',
    'port': '5432',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'uP(V~Oei]^@6tZ)d',
    'sslmode': 'verify-ca',
    'sslrootcert': POSTGRES_SSL_ROOT_CERT,
    'sslcert': POSTGRES_SSL_CERT,
    'sslkey': POSTGRES_SSL_KEY
}

# Source PostgreSQL table
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE", "investments.daily_stock_prices")

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 19),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1
}

# Create the DAG
dag = DAG(
    'postgres_to_trino_ssl_transfer',
    default_args=default_args,
    description='Transfer data from PostgreSQL to Trino with SSL',
    schedule_interval=None,  # Triggered manually
    catchup=False,
    tags=['postgres', 'trino', 'ssl', 'data_transfer'],
    params={
        'batch_size': Param(default=1000, type='integer', description='Number of records to process in each batch'),
        'limit': Param(default=-1, type='integer', description='Optional limit on number of records to transfer (-1 means no limit)')
    }
)

def verify_ssl_files():
    """Verify SSL certificate files exist."""
    missing_files = []
    for file_path in [POSTGRES_SSL_ROOT_CERT, POSTGRES_SSL_CERT, POSTGRES_SSL_KEY]:
        if not os.path.exists(file_path):
            missing_files.append(file_path)
    
    if missing_files:
        raise FileNotFoundError(f"SSL certificate files not found: {', '.join(missing_files)}")
    else:
        logger.info("SSL certificate files verified successfully")

def extract_load_data(**kwargs):
    """Extract data from PostgreSQL and load into Trino using SSL."""
    # Verify SSL files first
    verify_ssl_files()
    
    batch_size = kwargs['params'].get('batch_size', 1000)
    limit = kwargs['params'].get('limit', -1)
    
    postgres_conn = None
    trino_conn = None
    
    try:
        # Create PostgreSQL connection with SSL
        logger.info(f"Connecting to PostgreSQL at {DB_PARAMS['host']}:{DB_PARAMS['port']} with SSL")
        postgres_conn = psycopg2.connect(**DB_PARAMS)
        postgres_conn.autocommit = False
        
        # Create Trino connection
        logger.info(f"Connecting to Trino at {TRINO_HOST}:{TRINO_PORT}")
        trino_conn = trino.dbapi.connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog=TRINO_CATALOG,
            schema=TRINO_SCHEMA,
        )
        trino_cursor = trino_conn.cursor()
        
        # Get schema and table name parts for PostgreSQL
        schema_name = POSTGRES_TABLE.split('.')[0] if '.' in POSTGRES_TABLE else None
        table_name = POSTGRES_TABLE.split('.')[-1]
        full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name
        
        # Get count of records in PostgreSQL
        with postgres_conn.cursor() as pg_cursor:
            pg_cursor.execute(f"SELECT COUNT(*) FROM {full_table_name}")
            total_count = pg_cursor.fetchone()[0]
            if limit > 0 and limit < total_count:
                total_count = limit
        
        logger.info(f"Starting transfer of up to {total_count} records from PostgreSQL to Trino")
        
        # Process in batches
        offset = 0
        records_processed = 0
        
        while True:
            # Calculate batch size
            batch_query_size = batch_size
            if limit > 0:
                remaining = limit - records_processed
                if remaining <= 0:
                    break
                batch_query_size = min(batch_size, remaining)
            
            # Fetch data from PostgreSQL (excluding the 'id' column)
            query = f"""
            SELECT fetching_date, trade_date, company_code, ticker, price
            FROM {full_table_name}
            OFFSET {offset}
            LIMIT {batch_query_size}
            """
            
            logger.info(f"Fetching batch with offset {offset}, batch size {batch_query_size}")
            with postgres_conn.cursor() as pg_cursor:
                pg_cursor.execute(query)
                rows = pg_cursor.fetchall()
                column_names = [desc[0] for desc in pg_cursor.description]
            
            # Break if no more data
            if not rows:
                logger.info("No more data to fetch")
                break
            
            # Convert to DataFrame
            df = pd.DataFrame(rows, columns=column_names)
            logger.info(f"Fetched {len(df)} rows with columns: {', '.join(column_names)}")
            
            # Check if target table exists in Trino
            trino_cursor.execute(f"SHOW TABLES LIKE '{TRINO_TABLE}'")
            table_exists = trino_cursor.fetchone() is not None
            
            # Create table if it doesn't exist
            if not table_exists:
                logger.info(f"Creating target table {TRINO_TABLE}")
                columns_ddl = []
                for col_name, dtype in df.dtypes.items():
                    trino_type = "VARCHAR"  # Default type
                    if pd.api.types.is_integer_dtype(dtype):
                        trino_type = "BIGINT"
                    elif pd.api.types.is_float_dtype(dtype):
                        trino_type = "DOUBLE"
                    elif pd.api.types.is_datetime64_dtype(dtype):
                        trino_type = "TIMESTAMP"
                    elif pd.api.types.is_bool_dtype(dtype):
                        trino_type = "BOOLEAN"
                    
                    columns_ddl.append(f'"{col_name}" {trino_type}')
                
                create_table_sql = f"CREATE TABLE {TRINO_TABLE} ({', '.join(columns_ddl)})"
                trino_cursor.execute(create_table_sql)
                logger.info(f"Table {TRINO_TABLE} created successfully with columns: {', '.join(columns_ddl)}")
            else:
                # Verify that the target table has the expected columns
                trino_cursor.execute(f"DESCRIBE {TRINO_TABLE}")
                existing_columns = [row[0] for row in trino_cursor.fetchall()]
                missing_columns = set(column_names) - set(existing_columns)
                
                if missing_columns:
                    raise ValueError(f"Target table {TRINO_TABLE} is missing columns: {', '.join(missing_columns)}")
                else:
                    logger.info(f"Target table {TRINO_TABLE} has the expected columns")
            
            # Insert data into Trino
            logger.info(f"Inserting {len(df)} rows into {TRINO_TABLE}")
            placeholders = ', '.join(['?'] * len(column_names))  # Use ? for Trino
            columns_str = ', '.join([f'"{c}"' for c in column_names])
            insert_query = f"INSERT INTO {TRINO_TABLE} ({columns_str}) VALUES ({placeholders})"
            
            # Convert DataFrame to list of tuples for insertion
            data_tuples = [tuple(row) for row in df.values]
            for row in data_tuples:
                trino_cursor.execute(insert_query, row)
            
            logger.info(f"Batch inserted successfully")
            
            # Update counters
            records_processed += len(rows)
            offset += batch_size
            
            # Check if we're done
            if len(rows) < batch_query_size or (limit > 0 and records_processed >= limit):
                break
        
        logger.info(f"Transfer complete. Total records processed: {records_processed}")
        return records_processed
        
    except Exception as e:
        # Log error
        logger.error(f"Error during data transfer: {str(e)}")
        raise
        
    finally:
        # Close connections
        if 'trino_cursor' in locals() and trino_cursor:
            trino_cursor.close()
        if trino_conn:
            trino_conn.close()
        if postgres_conn and not postgres_conn.closed:
            postgres_conn.close()
        logger.info("Connections closed")

# Define the task
extract_load_task = PythonOperator(
    task_id='extract_load_data',
    python_callable=extract_load_data,
    provide_context=True,
    dag=dag,
)

# Task dependencies
extract_load_task