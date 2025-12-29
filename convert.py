def wrap_for_sproc(cleaned_lines, notebook_name):
    header = f"""
import uuid

def log_operation(session, status, error_message='', run_id=None, script_name=None):
    if run_id is None:
        run_id = str(uuid.uuid4())

    created_at = session.sql(
        "SELECT CURRENT_TIMESTAMP() AS created_at"
    ).collect()[0]["CREATED_AT"]

    log_df = session.create_dataframe([{{ 
        "run_id": run_id,
        "script_name": script_name,
        "status": status,
        "error_message": error_message,
        "created_at": created_at
    }}])

    log_df.write.save_as_table(
        "ORANGE_ZONE_SBX_TA.ML_MONITORING.OPERATION_LOGS",
        mode="append"
    )
    session.sql("COMMIT").collect()
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

    return textwrap.dedent(header + indented_code + footer)
