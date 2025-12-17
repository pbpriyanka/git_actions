from pathlib import Path

DIST_DIR = "dist"

for py in Path(DIST_DIR).glob("*.py"):
    print(f"Registering stored procedure for {py.name}")

print("Dummy stored procedure registration done")
