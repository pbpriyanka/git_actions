from pathlib import Path
import os

NOTEBOOK_DIR = "notebooks"
OUTPUT_DIR = "dist"

os.makedirs(OUTPUT_DIR, exist_ok=True)

for nb in Path(NOTEBOOK_DIR).glob("*.ipynb"):
    py_file = Path(OUTPUT_DIR) / f"{nb.stem}.py"

    py_file.write_text(f"""
def main():
    print("Converted from {nb.name}")
""")

    print(f"Converted {nb.name} → {py_file.name}")

print("Dummy notebook conversion completed")
