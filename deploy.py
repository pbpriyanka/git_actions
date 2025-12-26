import os
from snowflake.snowpark import Session
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

STAGE = '@"ORANGE_ZONE_SBX_TA"."PUBLIC"."CONNECTIONS"'
DATABASE = "ORANGE_ZONE_SBX_TA"
SCHEMA = "PUBLIC"


def get_session():
    private_key = serialization.load_pem_private_key(
        os.environ["SNOWFLAKE_PRIVATE_KEY"].encode(),
        password=None,
        backend=default_backend()
    )

    pkb = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    return Session.builder.configs({
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "role": os.environ["SNOWFLAKE_ROLE"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "private_key": pkb
    }).create()


def deploy():
    session = get_session()

    for file in os.listdir("scripts"):
        if not file.endswith(".py"):
            continue

        sp_name = file.replace(".py", "_sp")

        session.sql(f"""
            CREATE OR REPLACE PROCEDURE {DATABASE}.{SCHEMA}.{sp_name}()
            RETURNS STRING
            LANGUAGE PYTHON
            RUNTIME_VERSION = 3.11
            PACKAGES = ('snowflake-snowpark-python')
            HANDLER = 'run'
            EXECUTE AS OWNER
            AS
            $$
            from {file.replace('.py','')} import run
            def run_wrapper(session):
                run(session)
                return 'SUCCESS'
            $$
        """).collect()

        print(f"Deployed {sp_name}")

    session.close()


if __name__ == "__main__":
    deploy()
