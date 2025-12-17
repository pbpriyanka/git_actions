TASKS = ["task_1", "task_2", "task_3"]

previous = None

for task in TASKS:
    if previous:
        print(f"Creating {task} AFTER {previous}")
    else:
        print(f"Creating root task {task}")

    previous = task

print("Dummy task pipeline created")
