import os
import glob
from snowflake.connector import connect
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# -----------------------------
# Snowflake connection
# -----------------------------
private_key_pem = os.environ["SNOWFLAKE_PRIVATE_KEY"].encode()
private_key = serialization.load_pem_private_key(
    private_key_pem,
    password=None,
    backend=default_backend()
)
private_key_der = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

conn = connect(
    user=os.environ['SNOWFLAKE_USER'],
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
    database='ORANGE_ZONE_SBX_TA',
    schema='PUBLIC',
    private_key=private_key_der
)
cur = conn.cursor()

# -----------------------------
# Get SP names from scripts folder
# -----------------------------
SCRIPTS_DIR = "./scripts"
scripts = glob.glob(os.path.join(SCRIPTS_DIR, "*.py"))

# Extract base names without .py
sp_names = [os.path.splitext(os.path.basename(p))[0] for p in scripts]

# You can also create a task name by appending "_task"
task_names = [f"{name}_task" for name in sp_names]

# -----------------------------
# Create tasks in sequence
# -----------------------------
for i, (task_name, sp_name) in enumerate(zip(task_names, sp_names)):
    if i == 0:
        # First task with schedule
        sql = f"""
        CREATE OR REPLACE TASK {task_name}
          WAREHOUSE = {os.environ['SNOWFLAKE_WAREHOUSE']}
          SCHEDULE = 'USING CRON * * * * * UTC'  -- every minute
        AS
          CALL {sp_name}();
        """
    else:
        prev_task = task_names[i-1]
        sql = f"""
        CREATE OR REPLACE TASK {task_name}
          WAREHOUSE = {os.environ['SNOWFLAKE_WAREHOUSE']}
          AFTER {prev_task}
        AS
          CALL {sp_name}();
        """
    cur.execute(sql)

# Activate the first task
cur.execute(f"ALTER TASK {task_names[0]} RESUME")

print("Snowflake tasks created and pipeline activated!")

cur.close()
conn.close()
