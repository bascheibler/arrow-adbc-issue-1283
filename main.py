import snowflake_driver
from tables import Tables
from multiprocessing import get_context

# create list of tuples (schema, table) from the Tables dict
args = []
for schema_name, tables in Tables.items():
    for table_name in tables:
       args.append((schema_name, table_name))

if __name__ == '__main__':
    # upload sample data to Snowflake
    print("Uploading sample data to Snowflake. This can take 10 minutes.")

    snowflake_driver.upload_all_tables()
    # The function above unfortunately returns the error below after creating the table and inserting all values. Apparently it fails to commit.
    # File "/usr/local/lib/python3.12/site-packages/adbc_driver_manager/dbapi.py", line 894, in adbc_ingest
    #   return self._stmt.execute_update()
            #  ^^^^^^^^^^^^^^^^^^^^^^^^^^^
    # File "adbc_driver_manager/_lib.pyx", line 1184, in adbc_driver_manager._lib.AdbcStatement.execute_update
    # File "adbc_driver_manager/_lib.pyx", line 227, in adbc_driver_manager._lib.check_error
    # adbc_driver_manager.InternalError: INTERNAL: 261000 (08006): failed to POST. HTTP: 413, URL: <URL>

    # export tables in parallel (tested with 2 vCPU's)
    with get_context("spawn").Pool() as pool:
        pool.starmap(snowflake_driver.export_table, args)
    print("Done")