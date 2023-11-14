from os import getenv
from tables import Tables
from dotenv import load_dotenv

import pyarrow
import pyarrow.parquet as pq
import adbc_driver_manager
import adbc_driver_snowflake
import adbc_driver_snowflake.dbapi


COLS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

def format_uri() -> str:
    """Returns the URI string based on env variables and the .env file."""

    load_dotenv()
    user = getenv('SNOWFLAKE_USERNAME')
    password = getenv('SNOWFLAKE_PASSWORD')
    account = getenv('SNOWFLAKE_ACCOUNT')
    database = getenv('SNOWFLAKE_DATABASE')
    schema = getenv('SNOWFLAKE_SCHEMA')
    warehouse = getenv('SNOWFLAKE_WAREHOUSE')
    role = getenv('SNOWFLAKE_ROLE')
    return f"{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}"

snowflake_uri = format_uri()

def pyarrow_to_snowflake_type(pyarrow_type):
    """Map PyArrow data types to Snowflake column types"""

    if pyarrow.types.is_integer(pyarrow_type):
        return 'INT'
    if pyarrow.types.is_floating(pyarrow_type):
        return 'FLOAT'
    if pyarrow.types.is_string(pyarrow_type):
        return 'VARCHAR'
    if pyarrow.types.is_binary(pyarrow_type):
        return 'BINARY'
    if pyarrow.types.is_boolean(pyarrow_type):
        return 'BOOLEAN'
    if pyarrow.types.is_timestamp(pyarrow_type):
        return 'TIMESTAMP'
    if pyarrow.types.is_decimal(pyarrow_type):
        return 'DECIMAL'

def upload_table(Table, schema_name:str, table_name:str) -> None:
    """Ingests Parquet files into Snowflake tables."""

    with adbc_driver_snowflake.dbapi.connect(
        uri = snowflake_uri,
        db_kwargs = {
            "adbc.snowflake.sql.client_option.use_high_precision": "false"
        }
    ) as conn:
        with conn.cursor() as cursor:
            # Create empty table. Get schema from PyArrow table
            column_names, pyarrow_types = Table.schema.names, Table.schema.types
            snowflake_types = [pyarrow_to_snowflake_type(type) for type in pyarrow_types]
            schema = ', '.join([f"{name} {type}" for name, type in zip(column_names, snowflake_types)])
            query = f"create or replace table {schema_name}.{table_name} (parquet_raw variant)"
            cursor.execute(query)

            # Load files to table stage
            cursor.execute(f"PUT file:///app/samples/{table_name}/* @~/{table_name}/")

            # Copy data into table
            query = f"""
                copy into {schema_name}.{table_name}
                from @~/{table_name}/
                file_format = (type=parquet compression=snappy);"""
            cursor.execute(query)

            cols = ', '.join([f"parquet_raw:COL_{COLS[i]}" for i in range(len(Table.columns))])
            query = f"""
                create or replace table {schema_name}.{table_name}_download
                ({schema}) as
                select {cols}
                from {schema_name}.{table_name}"""
            cursor.execute(query)

def upload_all_tables() -> None:
    """Uploads to Snowflake all tables listed in tables.py."""

    for schema_name, tables in Tables.items():
        for table_name in tables:
            # Read .parquet file as PyArrow table
            Table = pq.read_table(f"/app/samples/{table_name}/")
            # Upload table to Snowflake
            upload_table(Table, schema_name, table_name)

def export_table(schema_name:str, table_name:str) -> None:
    """Fetches an entire table/view and saves it as a local Parquet dataset."""

    print(f"starting download of {schema_name}.{table_name}")
    query = f"select * from {schema_name}.{table_name}_download"

    with adbc_driver_snowflake.connect(
        uri = snowflake_uri,
        db_kwargs = {
            "adbc.snowflake.sql.client_option.use_high_precision": "false"
        }
    ) as db:
        with adbc_driver_manager.AdbcConnection(db) as conn:
            with adbc_driver_manager.AdbcStatement(conn) as stmt:
                stmt.set_options(
                    **{
                        adbc_driver_snowflake.StatementOptions.RESULT_QUEUE_SIZE.value: "200",
                        adbc_driver_snowflake.StatementOptions.PREFETCH_CONCURRENCY.value: "1"
                    }
                )
                stmt.set_sql_query(query)
                stream, _ = stmt.execute_query()
                reader = pyarrow.RecordBatchReader._import_from_c(stream.address)
                Table = reader.read_all()
                print(f"downloaded {schema_name}.{table_name}. {Table.num_rows} rows.")

    # Write PyArrow table to Parquet dataset:
    partitions = ["STUDYID", "STUDY_ID", "CMDM_STUDY_ID"]
    partition_cols = [col for col in partitions if col in Table.column_names]

    pq.write_to_dataset(
        Table,
        root_path = f"/app/data_lakehouse/{schema_name}/{table_name}",
        partition_cols = partition_cols)
    del Table

    print(f"exported {schema_name}.{table_name}")