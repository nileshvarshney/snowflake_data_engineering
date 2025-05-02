import time
from snowflake.snowpark import Session


# Constant variables
POS_TABLES = ['country', 'franchise', 'location', 'menu', 'truck', 'order_header', 'order_detail']
CUSTOMER_TABLES = ['customer_loyalty']
TABLE_DICT = {
    'pos': {"schema_name": 'RAW_POS', 'tables': POS_TABLES},
    'customer': {"schema_name": 'RAW_CUSTOMER', 'tables': CUSTOMER_TABLES}
}

def load_raw_table(session: Session, schema: str = None, table: str = None, year: int = None, s3dir: str = None):
    """
    Load data from S3 into Snowflake.
    """
    session.use_database("HOL_DB")
    session.use_schema(schema)

    if year:
        file_path = f"@external.FROSTBYTE_RAW_STAGE/{s3dir}/{table}/year={year}/"
    else:
        file_path = f"@external.FROSTBYTE_RAW_STAGE/{s3dir}/{table}/"

    print(file_path)

    # we can infer schema using the parquet read option
    print(f"Loading {table} from {file_path}")
    df = session.read.option("compression", "snappy").parquet(file_path)
    df.copy_into_table(f"{table}")
    print(f"Loaded {table} successfully from {file_path}")


def load_all_raw_data(session: Session):
    """
    Load all raw data from the specified table into Snowflake.
    """
    _ = session.sql("ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = 'MEDIUM' WAIT_FOR_COMPLETION = TRUE").collect()
    for s3dir, data in TABLE_DICT.items():
        schema = data['schema_name']
        tables = data['tables']
        for table in tables:
            print(f"Loading {table} from {s3dir} into {schema} schema")
            if table in ['order_header', 'order_detail']:
                for year in [2019, 2020, 2021]:
                    load_raw_table(session=session, schema=schema, table=table, year=year, s3dir=s3dir)
            else:
                load_raw_table(session=session, schema=schema, table=table, s3dir=s3dir)
    _ = session.sql("ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = 'XSMALL'").collect()


def validate_raw_tables(session: Session):
    """
    Validate that all raw tables have been loaded.
    """
    # check column names from the inferred schema
    for table in POS_TABLES:
        df = session.table(f"raw_pos.{table}")
        print(f"Table {table} has {len(df.columns)} columns")
        print(f"\n{table} \t{df.columns}\n")

    for table in CUSTOMER_TABLES:
        df = session.table(f"raw_customer.{table}")
        print(f"Table {table} has {len(df.columns)} columns")
        print(f"\n{table} \t{df.columns}\n")


if __name__ == "__main__":
    with Session.builder.getOrCreate() as session:
        load_all_raw_data(session)
        validate_raw_tables(session)
        print("All raw data loaded successfully.")
           