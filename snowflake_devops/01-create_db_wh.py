from snowflake.snowpark import Session
from snowflake.core import Root, CreateMode
from snowflake.core.database import Database
from snowflake.core.stage import Stage
from snowflake.core.warehouse import Warehouse
from snowflake.core.schema import Schema
from snowflake.core.table import Table, TableColumn



session = Session.builder.config("connection_name", "default").create()
root = Root(session)

def create_database(database_name = "python_api_db"):
  # Create a database
  database = root.databases.create(Database(name=database_name), mode=CreateMode.if_not_exists)
  return database

def create_schema(database, schema_name = "python_api_schema"):
  # Create a schema
  schema = database.schemas.create(Schema(name=schema_name), mode=CreateMode.if_not_exists)
  return schema

def create_warehouse(warehouse_name = "python_api_wh"):
   # Create a warehouse
    warehouse = root.warehouses.create(
       Warehouse(name=warehouse_name, size="XSMALL", auto_resume="true", auto_suspend=120), 
       mode=CreateMode.if_not_exists)
    return warehouse

def create_stage(database_name, schema_name, stage_name = "task_stage"):
   # get stages objects
   stages = root.databases[database_name].schemas[schema_name].stages
   stage = stages.create(Stage(name=stage_name, comment="stage for python api testing"), mode=CreateMode.if_not_exists)
   return stage

def truncate_table(database_name, schema_name, table_name):
    # Truncate a table
    table = root.databases[database_name].schemas[schema_name].tables[table_name]
    table.truncate()
    print(f"Table {table.name} truncated successfully.")
    
   

# # Create a table
# table = schema.tables.create(
#     Table(
#         name="python_api_table", 
#         columns=[
#            TableColumn(name="id", datatype="int"), 
#            TableColumn(name="name", datatype="string"),
#            ]
#            ),
#     mode=CreateMode.if_not_exists)


# # fetch the table details
# table_details = table.fetch()
# # print(table_details.to_dict())

# # append addtional columns to the table
# table_details.columns.append([TableColumn(name="age", datatype="int")])
# table.create_or_alter(table_details)


# # create a warehouse
# warehouse = root.warehouses.create(
#     Warehouse(name="python_api_wh", size="XSMALL", auto_resume=True, auto_suspend=120), 
#     mode=CreateMode.if_not_exists)


if __name__ == "__main__":
    # Create a database
    database = create_database("python_api_db")
    print(f"Database {database.name} created successfully.")
    
    # Create a schema
    schema = create_schema(database, schema_name="python_api_schema")
    print(f"Schema {schema.name} created successfully.")

    # Create a warehouse
    warehouse = create_warehouse(warehouse_name="python_api_wh")
    print(f"Warehouse {warehouse.name} created successfully.")
    # print(warehouse.fetch().to_dict())

    # Create a stage
    stage = create_stage(database_name=database.name, schema_name=schema.name, stage_name="task_stage")
    print(f"Stage {stage.name} created successfully.")
    
    session.close()


       