import sys
from snowflake.snowpark import Session
from snowflake.snowpark import functions as F


def table_exists(session: Session, schema: str ='', table_name: str ='') -> bool:
    exists = session.sql(
        f"""SELECT
          EXISTS(SELECT * from HOL_DB.INFORMATION_SCHEMA.TABLES 
          WHERE table_name = '{table_name}' and table_schema = '{schema}') AS TABLE_EXISTS""").collect()[0]['TABLE_EXISTS']
    return exists

def create_orders_table(session: Session):
    _=session.sql("USE DATABASE HOL_DB").collect()
    _=session.sql("USE SCHEMA HARMONIZED").collect()
    _= session.sql("CREATE OR REPLACE TABLE HOL_DB.HARMONIZED.ORDERS  LIKE HOL_DB.HARMONIZED.POS_FLATTENED_V").collect()
    _= session.sql("ALTER TABLE HOL_DB.HARMONIZED.ORDERS ADD COLUMN IF NOT EXISTS META_UPDATED_AT TIMESTAMP").collect()

def create_orders_table_stream(session: Session):
    _= session.sql("CREATE OR REPLACE STREAM HOL_DB.HARMONIZED.ORDERS_STREAM ON TABLE HOL_DB.HARMONIZED.ORDERS").collect()

def merge_order_update(session: Session):
    _= session.sql("ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = 'LARGE' WAIT_FOR_COMPLETION = TRUE").collect()
    source = session.table("HOL_DB.HARMONIZED.POS_FLATTENED_V_STREAM")
    target = session.table("HOL_DB.HARMONIZED.ORDERS")

    cols_to_update = {col: source[col] for col in source.schema.names if "METADATA" not in col}
    metadata_col_to_update = {"META_UPDATED_AT": F.current_timestamp()}
    updates = {**cols_to_update, **metadata_col_to_update}

    target.merge(source, target["ORDER_DETAIL_ID"] == source["ORDER_DETAIL_ID"],\
                 [F.when_matched().update(updates), F.when_not_matched().insert(updates)])

    _ = session.sql('ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XSMALL').collect()


def main(session: Session, *args)->str:
    _=session.sql("USE DATABASE HOL_DB").collect()
    if not table_exists(session, schema="HARMONIZED", table_name="ORDERS"):
        create_orders_table(session)
        create_orders_table_stream(session)

    return f"Order successfully processed"

if __name__ == "__main__":
    # Create a Snowflake session
    with Session.builder.getOrCreate() as session:
        session.sql("USE WAREHOUSE HOL_WH").collect()
        session.sql("USE SCHEMA HOL_DB.HARMONIZED").collect()
        if len(sys.argv) > 1:
            print(main(session, *sys.argv[1:]))
        else:
            print(main(session))
        