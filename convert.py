import os
import re
import subprocess
import nbformat

NOTEBOOK_DIR = "./notebooks"
SCRIPTS_DIR = "./scripts"
os.makedirs(SCRIPTS_DIR, exist_ok=True)


# --------------------------------------------------
# Step 1: Clean Databricks metadata
# --------------------------------------------------
def clean_notebook(notebook_path):
    with open(notebook_path, "r", encoding="utf-8") as f:
        nb = nbformat.read(f, as_version=4)

    for cell in nb.cells:
        cell.pop("id", None)
        cell["outputs"] = []
        cell["execution_count"] = None
        cell.get("metadata", {}).pop("application/vnd.databricks.v1+cell", None)

    cleaned = notebook_path.replace(".ipynb", "_cleaned.ipynb")
    with open(cleaned, "w", encoding="utf-8") as f:
        nbformat.write(nb, f)

    return cleaned


# --------------------------------------------------
# Step 2: Strip non-business logic
# --------------------------------------------------
def extract_logic(py_file):
    logic = []
    skip_patterns = [
        r"^import ",
        r"^from ",
        r"session\s*=",
        r"display\(",
        r"\.show\(",
        r"\.head\(",
        r"__main__",
    ]

    with open(py_file, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if any(re.search(p, stripped) for p in skip_patterns):
                continue
            logic.append(line)

    return logic


# --------------------------------------------------
# Step 3: Wrap into function
# --------------------------------------------------
def wrap_logic(logic_lines, name):
    body = "\n".join(f"    {l.rstrip()}" for l in logic_lines)
    return f"""
def run(session):
{body}
"""


# --------------------------------------------------
# Step 4: Convert one notebook
# --------------------------------------------------
def convert_notebook(nb_path):
    cleaned = clean_notebook(nb_path)
    name = os.path.splitext(os.path.basename(nb_path))[0]
    out_py = os.path.join(SCRIPTS_DIR, f"{name}.py")

    subprocess.run(
        ["jupyter", "nbconvert", "--to", "script", cleaned, "--output", name, "--output-dir", SCRIPTS_DIR],
        check=True
    )

    raw_py = out_py.replace(".py", ".txt")
    if os.path.exists(raw_py):
        os.rename(raw_py, out_py)

    logic = extract_logic(out_py)
    wrapped = wrap_logic(logic, name)

    with open(out_py, "w", encoding="utf-8") as f:
        f.write(wrapped)

    os.remove(cleaned)
    print(f"Converted {nb_path} → {out_py}")


# --------------------------------------------------
# Step 5: Convert all notebooks
# --------------------------------------------------
def convert_all():
    for f in os.listdir(NOTEBOOK_DIR):
        if f.endswith(".ipynb"):
            convert_notebook(os.path.join(NOTEBOOK_DIR, f))


if __name__ == "__main__":
    convert_all()
