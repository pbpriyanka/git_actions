from snowflake.connector import connect
import os

# Connect to Snowflake using environment variables
conn = connect(
    user=os.environ['SNOWFLAKE_USER'],
    password=os.environ['SNOWFLAKE_PASSWORD'],
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
    database='ORANGE_ZONE_SBX_TA',  # hardcoded
    schema='PUBLIC'       # hardcoded
)
cur = conn.cursor()

# List of SPs in execution order
sp_tasks = [
    ('data_harmonization_task', 'data_harmonization_sp'),
    ('missing_value_task', 'missing_value_sp'),
    # ('missing_dates_task', 'missing_dates'),
    ('feature_eng_task', 'feat_eng_sp'),
    ('training_task', 'training_sp')
]

for i, (task_name, sp_name) in enumerate(sp_tasks):
    if i == 0:
        # First task, no dependency
        sql = f"""
        CREATE OR REPLACE TASK {task_name}
          WAREHOUSE = {os.environ['SNOWFLAKE_WAREHOUSE']}
        AS
          CALL {sp_name}();
        """
    else:
        # Chain task after previous
        prev_task = sp_tasks[i-1][0]
        sql = f"""
        CREATE OR REPLACE TASK {task_name}
          WAREHOUSE = {os.environ['SNOWFLAKE_WAREHOUSE']}
          AFTER {prev_task}
        AS
          CALL {sp_name}();
        """
    cur.execute(sql)

# Activate the first task to start the pipeline
cur.execute(f"ALTER TASK {sp_tasks[0][0]} RESUME")

print("Snowflake tasks created and pipeline activated!")

# Close connection
cur.close()
conn.close()
