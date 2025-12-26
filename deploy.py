import os
from snowflake.snowpark import Session
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
STAGE = '@"ORANGE_ZONE_SBX_TA"."PUBLIC"."CONNECTIONS"'
DATABASE = "ORANGE_ZONE_SBX_TA"
SCHEMA = "PUBLIC"
REQUIREMENTS_FILE = "requirements.txt"


# --------------------------------------------------
# Snowflake session
# --------------------------------------------------
def get_session():
    private_key = serialization.load_pem_private_key(
        os.environ["SNOWFLAKE_PRIVATE_KEY"].encode(),
        password=None,
        backend=default_backend()
    )

    private_key_der = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    return Session.builder.configs({
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "role": os.environ["SNOWFLAKE_ROLE"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "private_key": private_key_der
    }).create()


# --------------------------------------------------
# Load PACKAGES dynamically from stage
# --------------------------------------------------
def load_packages_from_stage(session):
    target_dir = "/tmp"
    os.makedirs(target_dir, exist_ok=True)

    session.file.get(
        f'{STAGE}/{REQUIREMENTS_FILE}',
        target_dir
    )

    local_path = os.path.join(target_dir, REQUIREMENTS_FILE)

    packages = []
    with open(local_path, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                packages.append(line)

    # Mandatory Snowpark runtime
    if "snowflake-snowpark-python" not in packages:
        packages.append("snowflake-snowpark-python")

    return tuple(packages)


# --------------------------------------------------
# Deploy Stored Procedures
# --------------------------------------------------
def deploy():
    session = get_session()
    print("Snowflake session created")

    packages = load_packages_from_stage(session)
    print("Resolved PACKAGES:", packages)

    for file in os.listdir("scripts"):
        if not file.endswith(".py"):
            continue

        module_name = file.replace(".py", "")
        sp_name = f"{module_name}_sp"

        print(f"Deploying stored procedure: {sp_name}")

        session.sql(f"""
            CREATE OR REPLACE PROCEDURE {DATABASE}.{SCHEMA}.{sp_name}()
            RETURNS STRING
            LANGUAGE PYTHON
            RUNTIME_VERSION = 3.11
            PACKAGES = {packages}
            HANDLER = 'run_wrapper'
            EXECUTE AS OWNER
            AS
            $$
            from {module_name} import run

            def run_wrapper(session):
                run(session)
                return "SUCCESS"
            $$
        """).collect()

    session.close()
    print("All stored procedures deployed successfully")


# --------------------------------------------------
# Entry point
# --------------------------------------------------
if __name__ == "__main__":
    deploy()
