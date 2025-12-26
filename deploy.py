# deploy.py
import os
import glob
import textwrap
from snowflake.snowpark import Session
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# -----------------------------
# CONFIG
# -----------------------------
SCRIPTS_DIR = "./scripts"
PACKAGES = [
    "snowflake-snowpark-python",
    "pandas",
    "numpy",
    "snowflake-ml-python"
]

# -----------------------------
# SNOWFLAKE SESSION
# -----------------------------
def get_snowflake_session():
    private_key_pem = os.environ["SNOWFLAKE_PRIVATE_KEY"].encode()
    private_key = serialization.load_pem_private_key(
        private_key_pem, password=None, backend=default_backend()
    )
    private_key_der = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    connection_parameters = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "role": os.environ["SNOWFLAKE_ROLE"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "private_key": private_key_der
    }
    return Session.builder.configs(connection_parameters).create()

# -----------------------------
# CLEAN SCRIPT FOR SP
# -----------------------------
def prepare_script_for_sproc(script_path):
    """Return script content properly dedented for Snowflake SP."""
    with open(script_path, "r") as f:
        content = f.read()
    # Dedent everything to start at column 0
    content = textwrap.dedent(content)
    return content

# -----------------------------
# DEPLOY SP
# -----------------------------
def deploy_script(session, script_name, script_path):
    print(f"Deploying stored procedure: {script_name}")
    script_content = prepare_script_for_sproc(script_path)

    # Wrap the script in a run_wrapper function if needed
    sproc_code = f"""
def run_wrapper(session):
{textwrap.indent(script_content, '    ')}
    return "SUCCESS"
"""

    # Create SP
    sql = f"""
CREATE OR REPLACE PROCEDURE {script_name}()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ({', '.join(f"'{p}'" for p in PACKAGES)})
HANDLER = 'run_wrapper'
AS
$$
{sproc_code}
$$
"""
    # Debug: print SQL before executing
    print(sql)
    session.sql(sql).collect()
    print(f"{script_name} deployed successfully!\n")

# -----------------------------
# MAIN DEPLOY FUNCTION
# -----------------------------
def deploy():
    session = get_snowflake_session()
    # Set database and schema explicitly
    session.sql("USE DATABASE ORANGE_ZONE_SBX_TA").collect()
    session.sql("USE SCHEMA PUBLIC").collect()

    print("Snowflake session created\n")

    scripts = glob.glob(os.path.join(SCRIPTS_DIR, "*.py"))

    for script_path in scripts:
        script_name = os.path.splitext(os.path.basename(script_path))[0]
        deploy_script(session, script_name, script_path)

    session.close()
    print("All scripts deployed!")

# -----------------------------
# RUN LOCALLY
# -----------------------------
if __name__ == "__main__":
    deploy()
