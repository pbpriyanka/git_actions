import json
import os
import re
import subprocess
import uuid
import time
import traceback

# =========================================================
# CONFIGURATION
# =========================================================
NOTEBOOK_DIR = "./notebooks"  # folder containing notebooks
SCRIPTS_DIR = "./scripts"     # folder to save converted .py scripts
os.makedirs(SCRIPTS_DIR, exist_ok=True)


# =========================================================
# STEP 1: CLEAN DATABRICKS METADATA
# =========================================================
def clean_databricks_metadata(notebook_path):
    with open(notebook_path, "r", encoding="utf-8") as f:
        nb = json.load(f)

    for cell in nb.get("cells", []):
        cell.pop("id", None)
        if "metadata" in cell and "application/vnd.databricks.v1+cell" in cell["metadata"]:
            del cell["metadata"]["application/vnd.databricks.v1+cell"]

    cleaned_path = notebook_path.replace(".ipynb", "_cleaned.ipynb")
    with open(cleaned_path, "w", encoding="utf-8") as f:
        json.dump(nb, f)

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
    blocked = ("pyspark", "spark", "databricks", "display", "streamlit", "os", "sys")
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
            if "print(" in s or "display(" in s or "head(" in s:
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
from snowflake.snowpark import Session, functions as F
from snowflake.snowpark.functions import col, count, sum as sum_, countDistinct, coalesce, lit, when
import uuid
import time
import traceback
"""

    main_def = """
def log_operation(session, status, error_message='', run_id=None, script_name=None):
    if run_id is None:
        run_id = str(uuid.uuid4())
    created_at = session.sql("SELECT CURRENT_TIMESTAMP() AS created_at").collect()[0]["CREATED_AT"]
    log_df = session.create_dataframe([{
        "run_id": run_id,
        "script_name": script_name,
        "status": status,
        "error_message": error_message,
        "created_at": created_at
    }])
    log_df.write.save_as_table("ORANGE_ZONE_SBX_TA.ML_MONITORING.OPERATION_LOGS", mode="append")
    return run_id


def main(session):
    import os
    import sys
    script_name = os.path.basename(__file__)
    run_id = str(uuid.uuid4())
    log_operation(session, status="STARTED", run_id=run_id, script_name=script_name)
    try:
"""

    main_footer = """
        log_operation(session, status="SUCCESS", run_id=run_id, script_name=script_name)
        return session.create_dataframe([{
            "run_id": run_id,
            "script_name": script_name,
            "status": "SUCCESS",
            "error_message": None,
            "created_at": session.sql("SELECT CURRENT_TIMESTAMP() AS ts").collect()[0]["TS"]
        }])
    except Exception as e:
        error_msg = f"{str(e)}\\n{traceback.format_exc()}"
        log_operation(session, status="FAILED", error_message=error_msg, run_id=run_id, script_name=script_name)
        return session.create_dataframe([{
            "run_id": run_id,
            "script_name": script_name,
            "status": "FAILED",
            "error_message": error_msg,
            "created_at": session.sql("SELECT CURRENT_TIMESTAMP() AS ts").collect()[0]["TS"]
        }])


if __name__ == "__main__":
    connection_parameters = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA")
    }
    session = Session.builder.configs(connection_parameters).create()
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
    output_dir = os.path.dirname(notebook_path)
    subprocess.run(["jupyter", "nbconvert", "--to", "script", cleaned_ipynb, "--output-dir", output_dir], check=True)

    candidates = [f for f in os.listdir(output_dir) if f.endswith(".py") or f.endswith(".txt")]
    script_file = max(candidates, key=lambda f: os.path.getmtime(os.path.join(output_dir, f)))
    script_path = os.path.join(output_dir, script_file)
    if script_path.endswith(".txt"):
        new_path = script_path.replace(".txt", ".py")
        os.rename(script_path, new_path)
        script_path = new_path

    raw_imports = extract_imports(script_path)
    safe_imports = filter_safe_imports(raw_imports)
    cleaned_code = clean_script(script_path)

    notebook_name = os.path.splitext(os.path.basename(notebook_path))[0] + ".py"
    output_path = os.path.join(SCRIPTS_DIR, notebook_name)
    final_file = wrap_into_main(cleaned_code, safe_imports, output_path)

    os.remove(cleaned_ipynb)
    os.remove(script_path)

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
