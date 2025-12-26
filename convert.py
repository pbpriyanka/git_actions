import json
import os
import re
import subprocess
import nbformat

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
    """Fully sanitize Databricks notebook so nbconvert never fails."""
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
    session_patterns = [re.compile(r"^\s*session\s*=\s*get_active_session\(\s*\)")]
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
# STEP 5: WRAP INTO FINAL SCRIPT
# =========================================================
def wrap_into_main(cleaned_code, dynamic_imports, notebook_name, output_path):
    imports_block = "\n".join(dynamic_imports)
    indented_code = "\n".join("    " + line if line.strip() else "" for line in cleaned_code)
    final_script = f"{imports_block}\n\n# Notebook: {notebook_name}\n\ndef main():\n{indented_code}\n"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(final_script)
    return output_path

# =========================================================
# STEP 6: CONVERT ONE NOTEBOOK
# =========================================================
def convert_notebook(notebook_path):
    cleaned_ipynb = clean_databricks_metadata(notebook_path)
    notebook_name = os.path.splitext(os.path.basename(notebook_path))[0]
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

    if os.path.exists(output_py.replace(".py", ".txt")):
        os.rename(output_py.replace(".py", ".txt"), output_py)

    raw_imports = extract_imports(output_py)
    safe_imports = filter_safe_imports(raw_imports)
    cleaned_code = clean_script(output_py)

    final_file = wrap_into_main(cleaned_code, safe_imports, notebook_name, output_py)
    os.remove(cleaned_ipynb)
    print(f"Converted: {notebook_path} → {final_file}")
    return final_file

# =========================================================
# STEP 7: CONVERT ALL NOTEBOOKS
# =========================================================
def convert_all_notebooks():
    for f in os.listdir(NOTEBOOK_DIR):
        if f.endswith(".ipynb"):
            notebook_path = os.path.join(NOTEBOOK_DIR, f)
            convert_notebook(notebook_path)

if __name__ == "__main__":
    convert_all_notebooks()
