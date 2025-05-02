from snowflake.snowpark.session import Session
from snowflake.snowpark import functions as F


def create_pos_view(session: Session):
    """
    Create a view for the POS data.
    """
    session.use_schema("HOL_DB.HARMONIZED")
    order_detail = (
        session.table("RAW_POS.ORDER_DETAIL").select(
            F.col("ORDER_DETAIL_ID"),
            F.col("LINE_NUMBER"),
            F.col("MENU_ITEM_ID"),
            F.col("QUANTITY"),
            F.col("UNIT_PRICE"),
            F.col("PRICE"),
            F.col("ORDER_ID")
    ))

    order_header = (
        session.table("RAW_POS.ORDER_HEADER").select(
            F.col("ORDER_ID"),
            F.col("TRUCK_ID"),
            F.col("ORDER_TS"),
            F.to_date(F.col("ORDER_TS")).alias("ORDER_TS_DATE"),
            F.col("ORDER_AMOUNT"),
            F.col("ORDER_TAX_AMOUNT"),
            F.col("ORDER_DISCOUNT_AMOUNT"),
            F.col("ORDER_TOTAL"),
            F.col("LOCATION_ID")
    ))

    truck = (
        session.table("RAW_POS.TRUCK").select(
            F.col("TRUCK_ID"),
            F.col("PRIMARY_CITY"),
            F.col("REGION"),
            F.col("COUNTRY"),
            F.col("FRANCHISE_FLAG"),
            F.col("FRANCHISE_ID")
    ))

    menu = (
        session.table("RAW_POS.MENU").select(
            F.col("MENU_ITEM_ID"),
            F.col("MENU_ITEM_NAME"),
            F.col("MENU_TYPE"),
            F.col("TRUCK_BRAND_NAME")
    ))

    franhise = (
        session.table("RAW_POS.FRANCHISE").select(
            F.col("FRANCHISE_ID"),
            F.col("FIRST_NAME").alias("FRANCHISE_FIRST_NAME"),
            F.col("LAST_NAME").alias("FRANCHISE_LAST_NAME")
    ))

    location = (
        session.table("RAW_POS.LOCATION").select(
            F.col("LOCATION_ID")
    ))

    t_with_f = truck.join(franhise, truck.FRANCHISE_ID == franhise.FRANCHISE_ID, rsuffix='_f')
    oh_w_t_and_l = (
        order_header.join(t_with_f, order_header.TRUCK_ID == t_with_f.TRUCK_ID, rsuffix='_t')
        .join(location, order_header.LOCATION_ID == location.LOCATION_ID, rsuffix='_l')
    )
    final_df = (
        order_detail.join(oh_w_t_and_l, order_detail.ORDER_ID == oh_w_t_and_l.ORDER_ID,  rsuffix='_oh')
        .join(menu, order_detail.MENU_ITEM_ID == menu.MENU_ITEM_ID, rsuffix='_m')
        .select(
            F.col("ORDER_ID"),
            F.col("TRUCK_ID"),
             F.col("ORDER_TS"),
            F.col("ORDER_TS_DATE"),
            F.col("ORDER_DETAIL_ID"),
            F.col("LINE_NUMBER"),
            F.col("TRUCK_BRAND_NAME"),
            F.col("MENU_TYPE"),
            F.col("PRIMARY_CITY"),
            F.col("REGION"),
            F.col("COUNTRY"),
            F.col("FRANCHISE_FLAG"),
            F.col("FRANCHISE_ID"),
            F.col("FRANCHISE_FIRST_NAME"),
            F.col("FRANCHISE_LAST_NAME"),
            F.col("LOCATION_ID"),
            F.col("MENU_ITEM_ID"),
            F.col("MENU_ITEM_NAME"),
            F.col("QUANTITY"),
            F.col("UNIT_PRICE"),
            F.col("PRICE"),
            F.col("ORDER_AMOUNT"),
            F.col("ORDER_TAX_AMOUNT"),
            F.col("ORDER_DISCOUNT_AMOUNT"),
            F.col("ORDER_TOTAL")
        ) 
    )
    final_df.create_or_replace_view("pos_flattened_v")

def create_pos_view_stream(session: Session):
    session.use_schema("HOL_DB.HARMONIZED")
    # Create a stream for the order_header table
    _= (
        session.sql(
            "CREATE OR REPLACE STREAM pos_flattened_v_stream ON VIEW pos_flattened_v SHOW_INITIAL_ROWS = True"
        ).collect()
    )

def test_pos_view(session: Session):
    session.use_schema("HOL_DB.HARMONIZED")
    tv = session.table("pos_flattened_v")
    tv.limit(3).show()


if __name__ == "__main__":
    # Create a Snowpark session
    session = Session.builder.getOrCreate()
    # Use the database and schema
    session.use_database("HOL_DB")

    # create pos view
    create_pos_view(session)
    create_pos_view_stream(session)
#       test_pos_view(session)
