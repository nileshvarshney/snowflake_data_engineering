import time
from snowflake.snowpark import Session

POS_TABLES = ['country', 'franchise', 'location', 'menu', 'truck', 'order_header', 'order_detail']
CUSTOMER_TABLES = ['customer_loyality']
TABLE_DICT = {
    'pos': {"schema_name': 'pos', 'tables': POS_TABLES},"},
    'customer': CUSTOMER_TABLES
}