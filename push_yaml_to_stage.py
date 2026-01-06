import os
import tempfile
from snowflake.snowpark import Session
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

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
# UPLOAD YAML FILE
# -----------------------------
def upload_yaml_to_stage(session, yaml_path, stage='@"ORANGE_ZONE_SBX_TA"."PUBLIC"."MY_CSV_STAGE"'):
    print(f"Uploading {yaml_path} → {stage}")
    session.file.put(
        f"file://{yaml_path}",
        stage,   # No filename here
        auto_compress=False,
        overwrite=True
    )


# -----------------------------
# MAIN FUNCTION
# -----------------------------
if __name__ == "__main__":
    session = get_snowflake_session()
    session.sql("USE DATABASE ORANGE_ZONE_SBX_TA").collect()
    session.sql("USE SCHEMA PUBLIC").collect()

    upload_yaml_to_stage(session, "./notebooks/config_new_PROD.yaml")
    print("YAML uploaded successfully!")

    session.close()
