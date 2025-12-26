import os
from snowflake.connector import connect
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from deploy import deploy  # import deploy.py

# -----------------------------
# Get SP names from deploy.py
# -----------------------------
sp_names = deploy()  # returns list like ['data_harmonization', 'missing_value', ...]

# Optional: map to task names
sp_tasks = [(f"{sp}_task", sp) for sp in sp_names]

# -----------------------------
# Connect to Snowflake
# -----------------------------
private_key_pem = os.environ["SNOWFLAKE_PRIVATE_KEY"].encode()
private_key = serialization.load_pem_private_key(private_key_pem, password=None, backend=default_backend())
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
# Create tasks dynamically
# -----------------------------
for i, (task_name, sp_name) in enumerate(sp_tasks):
    if i == 0:
        sql = f"""
        CREATE OR REPLACE TASK {task_name}
          WAREHOUSE = {os.environ['SNOWFLAKE_WAREHOUSE']}
          SCHEDULE = 'USING CRON * * * * * UTC'
        AS
          CALL {sp_name}();
        """
    else:
        prev_task = sp_tasks[i-1][0]
        sql = f"""
        CREATE OR REPLACE TASK {task_name}
          WAREHOUSE = {os.environ['SNOWFLAKE_WAREHOUSE']}
          AFTER {prev_task}
        AS
          CALL {sp_name}();
        """
    cur.execute(sql)

# Activate first task
cur.execute(f"ALTER TASK {sp_tasks[0][0]} RESUME")

print("Snowflake tasks created and pipeline activated!")

cur.close()
conn.close()
