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


def upload_scripts_to_stage(scripts_folder="scripts", stage='@"ORANGE_ZONE_SBX_TA"."PUBLIC"."CONNECTIONS"'):
    session = get_snowflake_session()
    print("Snowflake session created successfully")

    # Check if folder exists
    if not os.path.exists(scripts_folder):
        print(f"Scripts folder '{scripts_folder}' does not exist. Skipping upload.")
        session.close()
        return

    files_uploaded = False
    for file in os.listdir(scripts_folder):
        if file.endswith(".py"):
            files_uploaded = True
            local_path = os.path.join(scripts_folder, file)
            print(f"Uploading {local_path} → {stage}")
            session.file.put(
                f"file://{os.path.abspath(local_path)}",
                f'{stage}/{file}',
                auto_compress=False,
                overwrite=True
            )

    if not files_uploaded:
        print(f"No Python scripts found in '{scripts_folder}'. Nothing uploaded.")
    else:
        print("All scripts uploaded to Snowflake stage successfully")

    session.close()


if __name__ == "__main__":
    upload_scripts_to_stage()
