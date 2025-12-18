import json
import os
import re
import subprocess
import uuid
import time
import traceback

NOTEBOOK_DIR = "notebooks"
SCRIPT_DIR = "scripts"

os.makedirs(SCRIPT_DIR, exist_ok=True)

# =========================================================
# STEP 1: CLEAN DATABRICKS METADATA FROM NOTEBOOK
# =========================================================
def clean_databricks_metadata(notebook_path):
    print("Cleaning Databricks metadata...")

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
    blocked = ("pyspark", "spark", "databricks", "display", "streamlit","os","sys")

    return [
        imp for imp in imports
        if not any(b in imp.lower() for b in blocked)
    ]

# =========================================================
# STEP 4: CLEAN SCRIPT LOGIC (REMOVE NOISE)
# =========================================================
def clean_script(script_path):
    print("Cleaning generated script...")

    cleaned_lines = []

    session_patterns = [
        re.compile(r"^\s*session\s*=\s*get_active_session\(\s*\)"),  # remove Snowflake session
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
                continue  # skip session/init lines

            cleaned_lines.append(line)

    return cleaned_lines

# =========================================================
# STEP 5: BUILD DYNAMIC HEADER + main(session) WITH LOGGING
# =========================================================
def build_dynamic_header(dynamic_imports, notebook_name):
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

    def log_operation(session, status, error_message='', run_id=None, script_name=script_name):
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

    start_time = time.time()
    run_id = str(uuid.uuid4())
    log_operation(session, status="STARTED", run_id=run_id, script_name=script_name)

    try:
"""

    main_footer = f"""
        log_operation(session, status="SUCCESS", run_id=run_id, script_name=script_name)

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
"""

    return imports_block + "\n" + snowflake_core + main_def, main_footer
def wrap_into_main(cleaned_code, dynamic_imports, notebook_path, output_path):
    notebook_name = os.path.basename(notebook_path)
    header, footer = build_dynamic_header(dynamic_imports, notebook_name)

    indented_code = "\n".join(
        "        " + line if line.strip() else "" for line in cleaned_code
    )

    final_script = header + "\n" + indented_code + footer

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(final_script)

    return output_path

# ------------------ Convert a single notebook ------------------
def convert_notebook(notebook_path, scripts_folder):
    print(f"\nConverting notebook: {notebook_path}")

    cleaned_ipynb = clean_databricks_metadata(notebook_path)
    output_dir = os.path.dirname(notebook_path)

    # Convert notebook → script
    subprocess.run(
        ["jupyter", "nbconvert", "--to", "script", cleaned_ipynb, "--output-dir", output_dir],
        check=True
    )

    # Detect generated script
    candidates = [
        f for f in os.listdir(output_dir)
        if f.endswith(".py") or f.endswith(".txt")
    ]

    script_file = max(
        candidates,
        key=lambda f: os.path.getmtime(os.path.join(output_dir, f))
    )
    script_path = os.path.join(output_dir, script_file)
    if script_path.endswith(".txt"):
        new_path = script_path.replace(".txt", ".py")
        os.rename(script_path, new_path)
        script_path = new_path

    # Extract imports & clean logic
    raw_imports = extract_imports(script_path)
    safe_imports = filter_safe_imports(raw_imports)
    cleaned_code = clean_script(script_path)

    # Output path inside scripts folder
    script_name = os.path.splitext(os.path.basename(notebook_path))[0] + ".py"
    output_path = os.path.join(scripts_folder, script_name)

    final_file = wrap_into_main(cleaned_code, safe_imports, notebook_path, output_path)

    # Cleanup temp files
    os.remove(cleaned_ipynb)
    os.remove(script_path)

    print(f"Generated script: {final_file}")
    return final_file

# ------------------ Convert all notebooks ------------------
def convert_all_notebooks(notebook_dir, scripts_folder):
    notebooks = [
        os.path.join(notebook_dir, f)
        for f in os.listdir(notebook_dir)
        if f.endswith(".ipynb")
    ]

    if not notebooks:
        print("No notebooks found in", notebook_dir)
        return []

    converted_files = []
    for nb in notebooks:
        converted_files.append(convert_notebook(nb, scripts_folder))

    return converted_files

# ------------------ Main Execution ------------------
if __name__ == "__main__":
    converted_files = convert_all_notebooks(NOTEBOOK_DIR, SCRIPT_DIR)
    print("\nAll notebooks converted:")
    for f in converted_files:
        print(f)
