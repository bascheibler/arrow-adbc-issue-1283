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

    # export tables in parallel (tested with 2 vCPU's)
    with get_context("spawn").Pool() as pool:
        pool.starmap(snowflake_driver.export_table, args)
    print("Done")