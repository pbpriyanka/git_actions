# upload_to_stage.py
import os
from snowflake.snowpark import Session
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

def get_snowflake_session():
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

    connection_parameters = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "role": os.environ["SNOWFLAKE_ROLE"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "private_key": private_key_der
    }

    return Session.builder.configs(connection_parameters).create()


def upload_scripts_to_stage(scripts_folder="scripts", stage='"ORANGE_ZONE_SBX_TA"."PUBLIC"."CONNECTIONS"'):
    session = get_snowflake_session()
    print("Snowflake session created successfully")

    for file in os.listdir(scripts_folder):
        if file.endswith(".py"):
            local_path = os.path.join(scripts_folder, file)
            print(f"Uploading {local_path} → {stage}")
            session.file.put(f"file://{local_path}", stage, auto_compress=False, overwrite=True)

    print("All scripts uploaded to Snowflake stage successfully")
    session.close()


if __name__ == "__main__":
    upload_scripts_to_stage()
