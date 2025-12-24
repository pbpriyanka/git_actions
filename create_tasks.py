import os
from snowflake.connector import connect
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# Load private key from environment variable (PEM format)
private_key_pem = os.environ["SNOWFLAKE_PRIVATE_KEY"].encode()

private_key = serialization.load_pem_private_key(
    private_key_pem,
    password=None,  # or password if your key is encrypted
    backend=default_backend()
)

private_key_der = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

# Connect to Snowflake using private key
conn = connect(
    user=os.environ['SNOWFLAKE_USER'],
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
    database='ORANGE_ZONE_SBX_TA',  # keep your hardcoded DB
    schema='PUBLIC',                 # keep your hardcoded schema
    private_key=private_key_der
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
        sql = f"""
        CREATE OR REPLACE TASK {task_name}
          WAREHOUSE = {os.environ['SNOWFLAKE_WAREHOUSE']}
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

# Activate the first task
cur.execute(f"ALTER TASK {sp_tasks[0][0]} RESUME")

print("Snowflake tasks created and pipeline activated!")

cur.close()
conn.close()
