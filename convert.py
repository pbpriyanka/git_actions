import os
import re
import nbformat
import subprocess
import textwrap

# =========================================================
# CONFIG
# =========================================================
NOTEBOOK_DIR = "./notebooks"
SCRIPTS_DIR = "./scripts"
os.makedirs(SCRIPTS_DIR, exist_ok=True)

# =========================================================
# STEP 1: CLEAN DATABRICKS METADATA
# =========================================================
def clean_databricks_metadata(notebook_path):
    with open(notebook_path, "r", encoding="utf-8") as f:
        nb = nbformat.read(f, as_version=4)

    for cell in nb.cells:
        cell.pop("id", None)
        if cell.cell_type != "code":
            cell.pop("outputs", None)
            cell.pop("execution_count", None)
        if cell.cell_type == "code":
            cell["outputs"] = []
            cell["execution_count"] = None
        if "metadata" in cell:
            cell["metadata"].pop("application/vnd.databricks.v1+cell", None)

    cleaned_path = notebook_path.replace(".ipynb", "_cleaned.ipynb")
    with open(cleaned_path, "w", encoding="utf-8") as f:
        nbformat.write(nb, f)

    return cleaned_path

# =========================================================
# STEP 2: EXTRACT CLEAN CODE FROM NOTEBOOK
# =========================================================
def extract_code(cleaned_ipynb, notebook_name):
    output_py = os.path.join(SCRIPTS_DIR, notebook_name + ".py")
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

    # Databricks sometimes exports as .txt
    txt_path = output_py.replace(".py", ".txt")
    if os.path.exists(txt_path):
        os.rename(txt_path, output_py)

    return output_py

# =========================================================
# STEP 3: CLEAN AND DEDENT SCRIPT
# =========================================================
def clean_script(script_path):
    lines = []
    session_patterns = [re.compile(r"^\s*session\s*=\s*get_active_session\(\s*\)")]
    with open(script_path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if s.startswith("import") or s.startswith("from"):
                lines.append(line)
            elif "display(" in s or "head(" in s:
                continue
            elif any(p.match(s) for p in session_patterns):
                continue
            else:
                lines.append(line)
    return lines

# =========================================================
# STEP 4: WRAP INTO RUN_WRAPPER WITH LOGGING
# =========================================================
def wrap_for_sproc(cleaned_lines, notebook_name):
    """
    Wrap notebook code into run_wrapper(session)
    Adds logging for both SUCCESS and FAILED cases
    """
    header = f"""
import uuid

def log_operation(session, status, error_message='', run_id=None, script_name=None):
    if run_id is None:
        run_id = str(uuid.uuid4())

    created_at = session.sql(
        "SELECT CURRENT_TIMESTAMP() AS created_at"
    ).collect()[0]["CREATED_AT"]

    log_df = session.create_dataframe([{
        "run_id": run_id,
        "script_name": script_name,
        "status": status,
        "error_message": error_message,
        "created_at": created_at
    }])

    log_df.write.save_as_table(
        "ORANGE_ZONE_SBX_TA.ML_MONITORING.OPERATION_LOGS",
        mode="append"
    )
    return run_id


def run_wrapper(session):
    run_id = str(uuid.uuid4())
    script_name = "{notebook_name}"
    try:
"""

    indented_code = textwrap.indent("".join(cleaned_lines), "        ")

    footer = """
        return log_operation(
            session=session,
            status="SUCCESS",
            run_id=run_id,
            script_name=script_name
        )
    except Exception as e:
        return log_operation(
            session=session,
            status="FAILED",
            error_message=str(e),
            run_id=run_id,
            script_name=script_name
        )
"""

    return header + indented_code + footer

# =========================================================
# STEP 5: CONVERT ONE NOTEBOOK
# =========================================================
def convert_notebook(notebook_path):
    notebook_name = os.path.splitext(os.path.basename(notebook_path))[0]

    cleaned_ipynb = clean_databricks_metadata(notebook_path)
    script_path = extract_code(cleaned_ipynb, notebook_name)
    cleaned_lines = clean_script(script_path)

    final_code = wrap_for_sproc(cleaned_lines, notebook_name)

    output_path = os.path.join(SCRIPTS_DIR, notebook_name + ".py")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(final_code)

    os.remove(cleaned_ipynb)

    print(f"Converted notebook: {notebook_path} → {output_path}")
    return output_path

# =========================================================
# STEP 6: CONVERT ALL NOTEBOOKS
# =========================================================
def convert_all_notebooks():
    for f in os.listdir(NOTEBOOK_DIR):
        if f.endswith(".ipynb"):
            convert_notebook(os.path.join(NOTEBOOK_DIR, f))

if __name__ == "__main__":
    convert_all_notebooks()
