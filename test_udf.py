import pandas as pd
import numpy as np
import datetime


# =====================================================
# Snowflake-safe conversion utilities
# =====================================================

def _to_python_scalar(x):
    if x is None or pd.isna(x):
        return None

    if isinstance(x, (np.integer,)):
        return int(x)

    if isinstance(x, (np.floating,)):
        return float(x)

    if isinstance(x, (np.bool_,)):
        return bool(x)

    if isinstance(x, pd.Timestamp):
        return x.to_pydatetime()

    if isinstance(x, (int, float, str, bool, datetime.date, datetime.datetime)):
        return x

    return str(x)


def convert_df_to_snowflake_safe(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        df[col] = df[col].apply(_to_python_scalar)
    return df


# =====================================================
# Safety assertion (THIS BLOCKS BAD DEPLOYS)
# =====================================================

def assert_snowflake_safe(df: pd.DataFrame):
    for col in df.columns:
        for val in df[col]:
            assert not isinstance(
                val, (np.generic, pd.Timestamp)
            ), f"❌ Unsafe type {type(val)} in column '{col}'"


# =====================================================
# Example UDF wrapper (plug in real UDF here)
# =====================================================

def missing_value_treatment_UDF(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace this body with your real logic.
    Keep the FINAL conversion step.
    """

    # Simulate real transformations
    df = df.copy()
    df["int_col"] = df["int_col"].fillna(0)
    df["float_col"] = df["float_col"].interpolate()
    df["bool_col"] = df["bool_col"].fillna(False)
    df["date_col"] = pd.to_datetime(df["date_col"])

    # 🚨 MANDATORY FINAL STEP
    df = convert_df_to_snowflake_safe(df)
    return df


# =====================================================
# ACTUAL TEST
# =====================================================

def test_missing_value_udf_snowflake_safe():
    df = pd.DataFrame({
        "int_col": pd.Series([1, None, 3], dtype="Int64"),
        "float_col": [1.2, None, 3.4],
        "str_col": ["a", None, "c"],
        "bool_col": pd.Series([True, None, False], dtype="boolean"),
        "date_col": pd.to_datetime(["2025-01-01", None, "2025-01-03"])
    })

    df_out = missing_value_treatment_UDF(df)

    # 1️⃣ Structural check
    assert isinstance(df_out, pd.DataFrame)

    # 2️⃣ NO numpy / pandas scalars allowed
    assert_snowflake_safe(df_out)

    # 3️⃣ Smoke print (debug-friendly)
    print("✅ Snowflake-safe output:")
    print(df_out)
    print(df_out.dtypes)


# =====================================================
# Local run support
# =====================================================

if __name__ == "__main__":
    test_missing_value_udf_snowflake_safe()
    print("🎉 ALL TESTS PASSED — SAFE TO DEPLOY")
