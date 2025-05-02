import sys
import os
import yaml

ignore_folfer = ['.git', '__pycache__', 'venv', '.ipynb_checkpoints']
snowflake_project_config_filename = 'snowflake.yaml'

if len(sys.argv) != 2:
    print("Root  directory of the project must be passed as an argument")
    sys.exit()

root_dir = sys.argv[1]
print(f"Deploying all Snowpark apps in root directory {root_dir}")

for (directory_path, directory_name, filenames) in os.walk(root_dir):
    base_name = os.path.basename(directory_path)
    
    # Check if the directory is in the ignore list  
    # and skip it if it is
    if base_name in ignore_folfer:
        continue

    # Check if the directory contains a snowflake.yaml file
    if not snowflake_project_config_filename in filenames:
        continue

    print(f"Found Snowpark project in {directory_path}")

    project_settings = {}
    with open(os.path.join(directory_path, snowflake_project_config_filename), "r") as f:
        project_settings = yaml.safe_load(f, Loader = yaml.FullLoader)

        # Check if the project is a Snowpark project
        if 'snowpark' not in project_settings:
            print(f"Skipping non-snowpark project in {directory_path}")
            continue

        print(f"Found Snowflake Snowpark project in '{project_settings['snowpark']['project_name']}' in folder {base_name}")
        print(f"Calling SnowCli for project deployment")

        os.chdir(directory_path)
        # Make sure all 6 SNOWFLAKE_ environment variables are set
        # SnowCLI accesses the passowrd directly from the SNOWFLAKE_PASSWORD environmnet variable
        os.system(f"snow snowpark build --temporary-connection --account $SNOWFLAKE_ACCOUNT --user $SNOWFLAKE_USER --role $SNOWFLAKE_ROLE --warehouse $SNOWFLAKE_WAREHOUSE --database $SNOWFLAKE_DATABASE")
        os.system(f"snow snowpark deploy --replace  --temporary-connection --account $SNOWFLAKE_ACCOUNT --user $SNOWFLAKE_USER --role $SNOWFLAKE_ROLE --warehouse $SNOWFLAKE_WAREHOUSE --database $SNOWFLAKE_DATABASE")

