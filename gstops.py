import os
import io
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from psycopg2 import sql

# --- Option A: Download Excel from Google Cloud Storage ---
def read_excel_from_gcs(bucket_name: str, blob_name: str, sheet_name=0):
    """
    Returns pandas DataFrame read from an Excel file stored in GCS.
    Assumes GOOGLE_APPLICATION_CREDENTIALS env var points to service account json or
    application default credentials are available.
    """
    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    bytes_data = blob.download_as_bytes()
    # read into DataFrame using pandas
    return pd.read_excel(io.BytesIO(bytes_data), sheet_name=sheet_name)

# --- Option B: Download Excel from AWS S3 ---
def read_excel_from_s3(bucket_name: str, key: str, aws_access_key_id=None, aws_secret_access_key=None, sheet_name=0, region_name=None):
    """
    Returns pandas DataFrame read from an Excel file in S3.
    """
    import boto3

    session_args = {}
    if aws_access_key_id and aws_secret_access_key:
        session_args['aws_access_key_id'] = aws_access_key_id
        session_args['aws_secret_access_key'] = aws_secret_access_key
    if region_name:
        session_args['region_name'] = region_name

    s3 = boto3.client('s3', **session_args)
    response = s3.get_object(Bucket=bucket_name, Key=key)
    bytes_data = response['Body'].read()
    return pd.read_excel(io.BytesIO(bytes_data), sheet_name=sheet_name)

# --- PostgreSQL insertion via SQLAlchemy (pandas.to_sql) ---
def load_df_to_postgres_sqlalchemy(df: pd.DataFrame, table_name: str, pg_uri: str, if_exists='append', chunksize=1000):
    """
    Insert DataFrame into PostgreSQL table using pandas.to_sql (SQLAlchemy).
    pg_uri example: postgresql+psycopg2://user:password@host:port/dbname
    """
    engine = create_engine(pg_uri)
    # to_sql will try to create the table if it doesn't exist (may require proper permissions)
    df.to_sql(name=table_name, con=engine, if_exists=if_exists, index=False, chunksize=chunksize, method='multi')
    engine.dispose()

# --- PostgreSQL insertion using psycopg2 COPY (fast for large loads) ---
def load_df_to_postgres_copy(df: pd.DataFrame, table_name: str, pg_conn_params: dict):
    """
    Fast bulk insert using COPY FROM STDIN. pg_conn_params: dict with keys dbname,user,password,host,port
    IMPORTANT: The order and names of DataFrame columns must match the target table or you should
    pass the 'columns' list to the COPY clause.
    """
    # Create CSV in memory without header (COPY expects data only)
    buffer = io.StringIO()
    # If your table expects exact column order, ensure df columns are in that order:
    # df = df[desired_column_order]
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    conn = psycopg2.connect(**pg_conn_params)
    cur = conn.cursor()

    try:
        # If column order matches DataFrame column order:
        copy_sql = sql.SQL("COPY {} FROM STDIN WITH CSV").format(sql.Identifier(table_name))
        cur.copy_expert(copy_sql.as_string(conn), buffer)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

# --- Example usage ---
if __name__ == "__main__":
    # --- CONFIG - change these for your environment ---
    # Choose cloud provider and file path:
    USE_GCS = True   # set False to use S3
    GCS_BUCKET = "my-gcs-bucket"
    GCS_BLOB = "path/to/myfile.xlsx"
    S3_BUCKET = "my-s3-bucket"
    S3_KEY = "path/to/myfile.xlsx"

    # Excel sheet to read (name or index)
    SHEET_NAME = 0

    # PostgreSQL connection
    # Option A: SQLAlchemy URI
    PG_SQLALCHEMY_URI = os.getenv("PG_SQLALCHEMY_URI", "postgresql+psycopg2://pg_user:pg_password@pg_host:5432/pg_database")
    # Option B: psycopg2 connection params (dict)
    PG_CONN_PARAMS = {
        "dbname": os.getenv("PG_DBNAME", "pg_database"),
        "user": os.getenv("PG_USER", "pg_user"),
        "password": os.getenv("PG_PASSWORD", "pg_password"),
        "host": os.getenv("PG_HOST", "pg_host"),
        "port": os.getenv("PG_PORT", "5432")
    }

    TARGET_TABLE = "schema.my_table"  # include schema if needed, e.g., public.my_table or schema.my_table

    # -------------------------
    # 1) Read Excel into DataFrame
    if USE_GCS:
        print("Reading from GCS...")
        df = read_excel_from_gcs(GCS_BUCKET, GCS_BLOB, sheet_name=SHEET_NAME)
    else:
        print("Reading from S3...")
        df = read_excel_from_s3(S3_BUCKET, S3_KEY, sheet_name=SHEET_NAME)

    print(f"Read DataFrame with shape: {df.shape}")

    # Optional: data cleaning / dtype conversions
    # df.columns = [c.strip() for c in df.columns]  # clean column names if needed

    # -------------------------
    # 2) Load into PostgreSQL
    # Choose method: 'to_sql' (SQLAlchemy) or 'copy' (psycopg2 COPY)
    LOAD_METHOD = os.getenv("LOAD_METHOD", "copy")  # 'to_sql' or 'copy'

    if LOAD_METHOD == "to_sql":
        print("Loading into Postgres via SQLAlchemy (to_sql)...")
        load_df_to_postgres_sqlalchemy(df, TARGET_TABLE, PG_SQLALCHEMY_URI, if_exists='append', chunksize=1000)
        print("Finished loading via to_sql.")
    else:
        print("Loading into Postgres via COPY (psycopg2)...")
        # If TARGET_TABLE includes schema (schema.table), psycopg2 COPY with sql.Identifier won't accept dot directly,
        # so split into schema and table:
        if "." in TARGET_TABLE:
            schema_name, table_only = TARGET_TABLE.split(".", 1)
            # create fully qualified name safely:
            full_table_name = f"{schema_name}.{table_only}"
        else:
            full_table_name = TARGET_TABLE

        # Note: load_df_to_postgres_copy expects the DataFrame's column order to match the database table columns.
        load_df_to_postgres_copy(df, full_table_name, PG_CONN_PARAMS)
        print("Finished loading via COPY.")
