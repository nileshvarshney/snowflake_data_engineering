from datetime import timedelta
from snowflake.snowpark import Session
from snowflake.core import Root, CreateMode
from snowflake.snowpark.functions import col
from snowflake.core.task import Task, StoredProcedureCall


DATABASE_NAME = "python_api_db"
SCHEMA_NAME = "python_api_schema"
WAREHOUSE_NAME = "python_api_wh"
ROLE_NAME = "accountadmin"

session = Session.builder.config("connection_name", "default").create()
session.sql(f"use role {ROLE_NAME}").collect()
session.sql(f"use database {DATABASE_NAME}").collect()
session.sql(f"use schema {SCHEMA_NAME}").collect()
session.sql(f"use warehouse {WAREHOUSE_NAME}").collect()

root = Root(session)

def truncate_table(session: Session, from_table: str, to_table: str, count) -> str:
    # Truncate a table
    session.table(from_table).limit(count).write.save_as_table(to_table)
    return "Truncate table created successfully"

def filter_by_shipmode(session: Session, shipmode: str) -> str:
    session.table("snowflake_sample_data.tpch_sf100.lineitem").filter(col("L_SHIPMODE") == shipmode).write.save_as_table("filter_table")
    return "Filter table successfully created!"


tasks_stage = f"{DATABASE_NAME}.{SCHEMA_NAME}.tasks_stage"

task1 = Task(
    name="task_python_api_truncate",
    definition=StoredProcedureCall(
        func=truncate_table,
        stage_location=f"@{tasks_stage}",
        packages=["snowflake-snowpark-python"]
    ),
    warehouse=WAREHOUSE_NAME,
    schedule=timedelta(minutes=1)
)

task2 = Task(
    name="task_python_api_filter",
    definition=StoredProcedureCall(
        func=filter_by_shipmode,
        stage_location=f"@{tasks_stage}",
        packages=["snowflake-snowpark-python"]
    ),
    warehouse=WAREHOUSE_NAME
)

# Create the tasks
tasks = root.databases[DATABASE_NAME].schemas[SCHEMA_NAME].tasks
truncate_task = tasks.create(task1, mode=CreateMode.or_replace)

task2.predecessors = [truncate_task.name]
filter_task = tasks.create(task2, mode=CreateMode.or_replace)

   
truncate_task.resume()

taskiter = tasks.iter()
for t in taskiter:
    print(f"Task name: {t.name} | State : {t.state}")

truncate_task.suspend()

truncate_task.drop()
filter_task.drop()
print("Tasks dropped successfully")
