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
        "private_key": private_key_der,
        "role": os.environ["SNOWFLAKE_ROLE"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"]
    }

    return Session.builder.configs(connection_parameters).create()


def main():
    session = get_snowflake_session()
    print("Connected to Snowflake")

    # CHANGE TABLE NAME HERE
    table_name = "ML_MONITORING.OPERATION_LOGS"

    df = session.table(table_name).limit(5)
    df.show()

    session.close()
    print("Table read completed")


if __name__ == "__main__":
    main()
