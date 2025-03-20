import os
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
import trino
import psycopg2
import psycopg2.extras
from io import StringIO

# Trino connection settings for input data
TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "airflow")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "stock_db")
COMPANY_TABLE = os.getenv("COMPANY_TABLE", "daily_stock_prices")

# PostgreSQL connection settings for output data
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin")
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
    'trino_to_postgres_transfer',
    default_args=default_args,
    description='Transfer data from Trino to PostgreSQL',
    schedule_interval=None,  # Triggered manually
    catchup=False,
    tags=['trino', 'postgres', 'data_transfer'],
    params={
        'batch_size': Param(default=10000, type='integer', description='Number of records to process in each batch'),
        'limit': Param(default=-1, type='integer', description='Optional limit on number of records to transfer (-1 means no limit)')
    }
)

def extract_load_data(**kwargs):
    """Extract data from Trino and load into PostgreSQL using psycopg2."""
    ti = kwargs['ti']
    batch_size = kwargs['params'].get('batch_size', 10000)
    limit = kwargs['params'].get('limit', -1)
    # Handle the limit param correctly (-1 means no limit)
    limit_clause = f"LIMIT {limit}" if limit > 0 else ""
    
    # Create direct Trino connection
    trino_conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
    )
    trino_cursor = trino_conn.cursor()
    
    # Create direct PostgreSQL connection with psycopg2
    postgres_conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    postgres_conn.autocommit = False  # For transaction management
    
    # Get schema and table name parts
    schema_name = POSTGRES_TABLE.split('.')[0] if '.' in POSTGRES_TABLE else None
    table_name = POSTGRES_TABLE.split('.')[-1]
    
    try:
        # Verify table exists and get column info
        print(f"Checking table structure for {COMPANY_TABLE}")
        table_info_query = f"SHOW COLUMNS FROM {COMPANY_TABLE}"
        trino_cursor.execute(table_info_query)
        columns_info = trino_cursor.fetchall()
        
        if not columns_info:
            raise Exception(f"Table {COMPANY_TABLE} appears to be empty or doesn't exist")
        
        print(f"Found {len(columns_info)} columns in the source table")
        
        # Get the total count of records (for logging)
        count_query = f"SELECT COUNT(*) as total_count FROM {COMPANY_TABLE}"
        trino_cursor.execute(count_query)
        total_count = trino_cursor.fetchone()[0]
        if limit > 0 and limit < total_count:
            total_count = limit
        
        print(f"Starting transfer of up to {total_count} records from Trino to PostgreSQL")
        
        # Create the schema if it doesn't exist
        if schema_name:
            with postgres_conn.cursor() as pg_cursor:
                pg_cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                postgres_conn.commit()
        
        # Process in batches to avoid memory issues
        offset = 0
        records_processed = 0
        
        while True:
            # Extract data from Trino in batches - don't try to sort by specific columns
            batch_query_size = batch_size
            if limit > 0:
                # If overall limit is specified, adjust the batch size for the last batch
                remaining = limit - records_processed
                if remaining <= 0:
                    break
                batch_query_size = min(batch_size, remaining)
            
            # Query without assuming column names for sorting
            query = f"""
            SELECT * FROM {COMPANY_TABLE}
            OFFSET {offset}
            LIMIT {batch_query_size}
            """
            
            # Execute the query and get data
            print(f"Fetching batch with offset {offset}, batch size {batch_query_size}")
            trino_cursor.execute(query)
            
            # Get column names
            column_names = [desc[0] for desc in trino_cursor.description]
            
            # Fetch data
            rows = trino_cursor.fetchall()
            
            # If no more data, break the loop
            if not rows:
                break
            
            # Convert to pandas DataFrame for easier handling
            df = pd.DataFrame(rows, columns=column_names)
            print(f"Fetched {len(df)} rows with columns: {', '.join(column_names)}")
            
            # Load data into PostgreSQL
            print(f"Loading {len(df)} records into PostgreSQL")
            
            with postgres_conn.cursor() as pg_cursor:
                # Check if target table exists, if not create it based on source table structure
                full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name
                pg_cursor.execute(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = %s AND table_name = %s
                    )
                """, (schema_name or 'public', table_name))
                
                table_exists = pg_cursor.fetchone()[0]
                
                if not table_exists:
                    print(f"Creating target table {full_table_name}")
                    # Create columns DDL from DataFrame for PostgreSQL
                    columns_ddl = []
                    for col_name, dtype in df.dtypes.items():
                        pg_type = "TEXT"  # Default type
                        if pd.api.types.is_integer_dtype(dtype):
                            pg_type = "BIGINT"
                        elif pd.api.types.is_float_dtype(dtype):
                            pg_type = "DOUBLE PRECISION"
                        elif pd.api.types.is_datetime64_dtype(dtype):
                            pg_type = "TIMESTAMP"
                        elif pd.api.types.is_bool_dtype(dtype):
                            pg_type = "BOOLEAN"
                        
                        columns_ddl.append(f"\"{col_name}\" {pg_type}")
                    
                    create_table_sql = f"CREATE TABLE {full_table_name} ({', '.join(columns_ddl)})"
                    pg_cursor.execute(create_table_sql)
                    postgres_conn.commit()
                
                # Prepare the INSERT statement
                placeholders = ', '.join(['%s'] * len(column_names))
                columns_str = ', '.join([f'"{c}"' for c in column_names])
                insert_query = f"INSERT INTO {full_table_name} ({columns_str}) VALUES ({placeholders})"
                
                # Execute batch insert
                psycopg2.extras.execute_batch(
                    pg_cursor, 
                    insert_query,
                    df.values.tolist()
                )
                postgres_conn.commit()
            
            # Update counters
            records_processed += len(rows)
            offset += batch_size
            
            # If we've processed all records or hit the limit, break
            if len(rows) < batch_query_size or (limit > 0 and records_processed >= limit):
                break
        
        print(f"Transfer complete. Total records processed: {records_processed}")
        return records_processed
        
    except Exception as e:
        # Rollback on error
        if postgres_conn and not postgres_conn.closed:
            postgres_conn.rollback()
        print(f"Error during data transfer: {str(e)}")
        raise
        
    finally:
        # Close connections
        if 'trino_cursor' in locals() and trino_cursor:
            trino_cursor.close()
        if 'trino_conn' in locals() and trino_conn:
            trino_conn.close()
        if 'postgres_conn' in locals() and postgres_conn and not postgres_conn.closed:
            postgres_conn.close()

# Define the task
extract_load_task = PythonOperator(
    task_id='extract_load_data',
    python_callable=extract_load_data,
    provide_context=True,
    dag=dag,
)

# Task dependencies (only one task in this DAG)
extract_load_task