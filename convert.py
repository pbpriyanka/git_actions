import json
import os
import re
import subprocess
import uuid
import time
import traceback
import nbformat

# =========================================================
# CONFIGURATION
# =========================================================
NOTEBOOK_DIR = "./notebooks"  # folder containing notebooks
SCRIPTS_DIR = "./script"     # folder to save converted .py scripts
os.makedirs(SCRIPTS_DIR, exist_ok=True)


# =========================================================
# STEP 1: CLEAN DATABRICKS METADATA
# =========================================================
def clean_databricks_metadata(notebook_path):

    """Fully sanitize Databricks notebook so nbconvert never fails."""
    with open(notebook_path, "r", encoding="utf-8") as f:
        nb = nbformat.read(f, as_version=4)

    for cell in nb.cells:
        # Remove problematic IDs
        cell.pop("id", None)

        # Markdown / raw cells must not have outputs
        if cell.cell_type != "code":
            cell.pop("outputs", None)
            cell.pop("execution_count", None)

        # Code cells: outputs are allowed but can be safely removed
        if cell.cell_type == "code":
            cell["outputs"] = []
            cell["execution_count"] = None

        # Remove Databricks-specific metadata
        if "metadata" in cell:
            cell["metadata"].pop("application/vnd.databricks.v1+cell", None)

    cleaned_path = notebook_path.replace(".ipynb", "_cleaned.ipynb")
    with open(cleaned_path, "w", encoding="utf-8") as f:
        nbformat.write(nb, f)

    return cleaned_path


# =========================================================
# STEP 2: EXTRACT IMPORTS DYNAMICALLY
# =========================================================
def extract_imports(script_path):
    imports = set()
    pattern = re.compile(r"^\s*(import|from)\s+[a-zA-Z0-9_\.]+")
    with open(script_path, "r", encoding="utf-8") as f:
        for line in f:
            if pattern.match(line.strip()):
                imports.add(line.strip())
    return sorted(imports)


# =========================================================
# STEP 3: FILTER SNOWFLAKE-SAFE IMPORTS
# =========================================================
def filter_safe_imports(imports):
    blocked = (
        "pyspark",
        "spark",
        "databricks",
        "dbutils",
        "display",
        "streamlit",
        "snowflake.snowpark.context",
        "get_active_session"
    )
    return [imp for imp in imports if not any(b in imp.lower() for b in blocked)]



# =========================================================
# STEP 4: CLEAN SCRIPT LOGIC
# =========================================================
def clean_script(script_path):
    cleaned_lines = []
    session_patterns = [
        re.compile(r"^\s*session\s*=\s*get_active_session\(\s*\)")
    ]
    with open(script_path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if s.startswith("import") or s.startswith("from"):
                continue
            if "display(" in s or "head(" in s:
                continue
            if any(p.match(s) for p in session_patterns):
                continue
            cleaned_lines.append(line)
    return cleaned_lines


# =========================================================
# STEP 5: BUILD HEADER AND MAIN WITH LOGGING & RETURN DF
# =========================================================
def build_dynamic_header(dynamic_imports):
    imports_block = "\n".join(dynamic_imports)

    snowflake_core = """
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import functions as F
import uuid
import time
import traceback
"""

    main_def = f"""
def main(session):
    import os
    import sys

    script_name = "{notebook_name}"  # Use the original notebook name

    def log_operation(session, status, error_message='', run_id=None, script_name=None):
        if run_id is None:
            run_id = str(uuid.uuid4())
        created_at = session.sql("SELECT CURRENT_TIMESTAMP() AS created_at").collect()[0]["CREATED_AT"]
        log_df = session.create_dataframe([{{
            "run_id": run_id,
            "script_name": script_name,
            "status": status,
            "error_message": error_message,
            "created_at": created_at
        }}])
        log_df.write.save_as_table("ORANGE_ZONE_SBX_TA.ML_MONITORING.OPERATION_LOGS", mode="append")
        return run_id

    def log_script(session: snowpark.Session,script_name=script_name):

        run_id = str(uuid.uuid4())
        try:
            # Log START
            log_operation(
                session,
                status="STARTED",
                run_id=run_id,
                script_name=script_name
            )

            # Log SUCCESS
            log_operation(
                session,
                status="SUCCESS",
                run_id=run_id,
                script_name=script_name
            )

            # ALWAYS return a DataFrame
            return session.table(
    "ORANGE_ZONE_SBX_TA.ML_MONITORING.OPERATION_LOGS"
).filter(
    F.col("RUN_ID") == run_id
).select(
    F.col("RUN_ID").alias("run_id"),
    F.col("MODEL_NAME").alias("script_name"),   # alias to match return_schema
    F.col("STATUS").alias("status"),
    F.col("ERROR_MESSAGE").alias("error_message"),
    F.col("CREATED_AT").alias("created_at")
)


        except Exception as e:
            error_message = f"{{str(e)}}\\n{{traceback.format_exc()}}"

            # Log FAILURE
            log_operation(
                session,
                status="FAILED",
                error_message=error_message,
                run_id=run_id,
                script_name=script_name
            )

            return session.create_dataframe(
    [{{
        "run_id": run_id,
        "script_name": script_name,
        "status": "FAILED",
        "error_message": error_message,
        "created_at": session.sql("SELECT CURRENT_TIMESTAMP() AS ts").collect()[0]["TS"]
    }}],
    schema=return_schema
)      
    packages = []
    stage_file_path = '@"ORANGE_ZONE_SBX_TA"."PUBLIC"."CONNECTIONS"/requirements.txt'
    target_dir = "/tmp"
    session.file.get(stage_file_path, target_dir)

    file_name = stage_file_path.split("/")[-1]
    local_path = os.path.join(target_dir, file_name)

    with open(local_path, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                packages.append(line)

    if "snowflake-snowpark-python" not in packages:
        packages.append("snowflake-snowpark-python")

    # --------------------------
    # Stored Procedure return schema
    # --------------------------
    return_schema = StructType([
        StructField("run_id", StringType()),
        StructField("script_name", StringType()),
        StructField("status", StringType()),
        StructField("error_message", StringType()),
        StructField("created_at", StringType())
    ])

    start_time = time.time()
    run_id = str(uuid.uuid4())
    log_operation(session, status="STARTED", run_id=run_id, script_name=script_name)

    try:
"""

    main_footer = f"""
        log_operation(session, status="SUCCESS", run_id=run_id, script_name=script_name)

        session.sproc.register(
            func=log_script,
            name=script_name,
            packages=packages,
            replace=True,
            is_permanent=True,
            stage_location='@"CONNECTIONS"',
            database="ORANGE_ZONE_SBX_TA",
            schema="PUBLIC",
            return_type=return_schema
        )

        return session.create_dataframe([{{
            "run_id": run_id,
            "script_name": script_name,
            "status": "SUCCESS",
            "error_message": None,
            "created_at": session.sql("SELECT CURRENT_TIMESTAMP() AS ts").collect()[0]["TS"]
        }}])

    except Exception as e:
        error_msg = f"{{str(e)}}\\n{{traceback.format_exc()}}"
        log_operation(session, status="FAILED", error_message=error_msg, run_id=run_id, script_name=script_name)
        return session.create_dataframe([{{
            "run_id": run_id,
            "script_name": script_name,
            "status": "FAILED",
            "error_message": error_msg,
            "created_at": session.sql("SELECT CURRENT_TIMESTAMP() AS ts").collect()[0]["TS"]
        }}])

if __name__ == "__main__":
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
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
    }

    session = Session.builder.configs(connection_parameters).create()
    print("Snowflake session created successfully")


    result_df = main(session)
    result_df.show()
    session.close()
"""
    return imports_block + "\n" + snowflake_core + main_def, main_footer

# =========================================================
# STEP 6: WRAP INTO FINAL SCRIPT
# =========================================================
def wrap_into_main(cleaned_code, dynamic_imports, output_path):
    header, footer = build_dynamic_header(dynamic_imports)
    indented_code = "\n".join("        " + line if line.strip() else "" for line in cleaned_code)
    final_script = header + "\n" + indented_code + footer
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(final_script)
    return output_path


# =========================================================
# STEP 7: CONVERT ONE NOTEBOOK
# =========================================================
def convert_notebook(notebook_path):
    cleaned_ipynb = clean_databricks_metadata(notebook_path)

    notebook_name = os.path.splitext(os.path.basename(notebook_path))[0]
    output_py = os.path.join(SCRIPTS_DIR, notebook_name + ".py")

    # Convert notebook → script directly into scripts directory
    subprocess.run(
        [
            "jupyter", "nbconvert",
            "--to", "script",
            cleaned_ipynb,
            "--output", notebook_name,
            "--output-dir", SCRIPTS_DIR
        ],
        check=True
    )

    # nbconvert sometimes creates .txt instead of .py
    if os.path.exists(output_py.replace(".py", ".txt")):
        os.rename(output_py.replace(".py", ".txt"), output_py)

    raw_imports = extract_imports(output_py)
    safe_imports = filter_safe_imports(raw_imports)
    cleaned_code = clean_script(output_py)

    final_file = wrap_into_main(cleaned_code, safe_imports, output_py)

    os.remove(cleaned_ipynb)

    print(f"Converted: {notebook_path} → {final_file}")
    return final_file


# =========================================================
# STEP 8: CONVERT ALL NOTEBOOKS
# =========================================================
def convert_all_notebooks():
    for f in os.listdir(NOTEBOOK_DIR):
        if f.endswith(".ipynb"):
            notebook_path = os.path.join(NOTEBOOK_DIR, f)
            convert_notebook(notebook_path)


if __name__ == "__main__":
    convert_all_notebooks()
