import pandas as pd
import numpy as np

def convert_df_to_snowflake_safe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert all pandas DataFrame columns to native Python types
    for Snowflake-safe output from UDF.
    """
    for col in df.columns:
        dtype = df[col].dtype

        # Integers
        if pd.api.types.is_integer_dtype(dtype):
            df[col] = df[col].apply(lambda x: int(x) if pd.notnull(x) else None)
        
        # Floats
        elif pd.api.types.is_float_dtype(dtype):
            df[col] = df[col].apply(lambda x: float(x) if pd.notnull(x) else None)

        # Strings (convert pandas string dtype or object to native str)
        elif pd.api.types.is_string_dtype(dtype) or pd.api.types.is_object_dtype(dtype):
            df[col] = df[col].apply(lambda x: str(x) if pd.notnull(x) else None)

        # Booleans
        elif pd.api.types.is_bool_dtype(dtype):
            df[col] = df[col].apply(lambda x: bool(x) if pd.notnull(x) else None)

        # Dates/Datetime
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            df[col] = df[col].apply(lambda x: pd.Timestamp(x) if pd.notnull(x) else None)

    return df


# Paste your missing_value_treatment_UDF here
def missing_value_treatment_UDF(df: pd.DataFrame) -> pd.DataFrame:
    """Function utilizing broadcasted information from the config file to treat the missing values in the input dataset

    Parameters
    ----------
    df : pd.DataFrame
        the raw dataset which contains the value for all variables

    Returns
    -------
    pd.DataFrame
        Returns the input dataframe after the missing values in them are treated
    """
    df = df.sort_values(by=[broadcast_date_col],ascending=True)
  
    algo_params = broadcast_algo_params
    modeling_granularity = broadcast_granularity
    req_params = dict([x for x in broadcast_algo_params.items() if len(x[1]['cols'])>0])  
    for algo in req_params.keys():
        if algo in ['Rolling_Mean']:
            window = int(algo_params[algo]['window'])
            cols = algo_params[algo]['cols']
            if algo_params[algo]['zero_as_missing_value'] == True:
                df[cols] = df[cols].replace(0,np.nan)
            df = impute_missing_data(df, cols, algo, window = window)
        elif algo in ['Rolling_Median']:
            window = int(algo_params[algo]['window'])
            cols = algo_params[algo]['cols']
            if algo_params[algo]['zero_as_missing_value'] == True:
                df[cols] = df[cols].replace(0,np.nan)
            df = impute_missing_data(df, cols, algo, window = window)
        elif algo in ['Scalar']:
            value = int(algo_params[algo]['value'])
            cols = algo_params[algo]['cols']
            if algo_params[algo]['zero_as_missing_value'] == True:
                df[cols] = df[cols].replace(0,np.nan)
            df = impute_missing_data(df, cols, algo, arbitrary_value = value)
        elif algo in ['Forward_fill']:
            cols = algo_params[algo]['cols']
            df = impute_missing_data(df, cols, algo) 
        elif algo in ['Backward_fill']:
            cols = algo_params[algo]['cols']
            if algo_params[algo]['zero_as_missing_value'] == True:
                df[cols] = df[cols].replace(0,np.nan)
            df = impute_missing_data(df, cols, algo)
        elif algo in ['Linear_Interpolation']:
            cols = algo_params[algo]['cols']
            if algo_params[algo]['zero_as_missing_value'] == True:
                df[cols] = df[cols].replace(0,np.nan)
            df = impute_missing_data(df, cols, algo)
        elif algo in ['Spline_Interpolation']:
            cols = algo_params[algo]['cols']
            if algo_params[algo]['zero_as_missing_value'] == True:
                df[cols] = df[cols].replace(0,np.nan)
            df = impute_missing_data(df, cols, algo)
        elif algo in ['Mean']:
            cols = algo_params[algo]['cols']
            if algo_params[algo]['zero_as_missing_value'] == True:
                df[cols] = df[cols].replace(0,np.nan)
            df = impute_missing_data(df, cols, algo)
        elif algo in ['Median']:
            cols = algo_params[algo]['cols']
            if algo_params[algo]['zero_as_missing_value'] == True:
                df[cols] = df[cols].replace(0,np.nan)
            df = impute_missing_data(df, cols, algo)
        elif algo in ['Mode']:
            cols = algo_params[algo]['cols']
            df = impute_missing_data(df, cols, algo)
        elif algo in ['Zero']:
            cols = algo_params[algo]['cols']
            df = impute_missing_data(df, cols, algo)
        elif algo in ['Mean_Across_Years']:
            time_granularity = algo_params[algo]['time_granularity']
            cols = algo_params[algo]['cols']
            if algo_params[algo]['zero_as_missing_value'] == True:
                df[cols] = df[cols].replace(0,np.nan)
            df = impute_missing_data(df, cols, algo, modeling_granularity = modeling_granularity, time_granularity = time_granularity, date_col = broadcast_date_col.value)
    df = convert_df_to_snowflake_safe(df)
    return df
     
# Include convert_df_to_snowflake_safe function

def test_udf():
    # Create a sample DataFrame with representative edge cases
    df = pd.DataFrame({
        'int_col': [1, 2, None, 4],
        'float_col': [1.5, None, 3.2, 4.0],
        'str_col': ['a', None, 'c', 'd'],
        'bool_col': [True, False, None, True],
        'date_col': pd.to_datetime(['2025-01-01', None, '2025-01-03', '2025-01-04'])
    })

    # Run your UDF
    df_out = missing_value_treatment_UDF(df)

    # Print output dtypes and values
    print(df_out.dtypes)
    print(df_out)
    
if __name__ == "__main__":
    test_udf()
