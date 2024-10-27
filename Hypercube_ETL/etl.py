import pandas as pd
import numpy as np
import json

# Extract the CSV data named "bmrs_wind_forecast_pair.csv"
bmrs_df = pd.read_csv('data/bmrs_wind_forecast_pair.csv')
with open('data/linear_orders_raw.json', 'r') as f:
    linear_order_df = json.load(f)



# Assessing the data quality for bmrs
def check_data_quality(bmrs_df):
    print("Checking data quality: ")
    print(f"Total number of rows: {len(bmrs_df)}")
    print(f"Missing values per column:")
    print(bmrs_df.isnull().sum())
    
# Transform the bmrs_df data based on requirements
def transform_data(bmrs_df):
    #Identifying the numerical columns
    numerical_columns = bmrs_df.select_dtypes(include=[np.number]).columns
    for col in numerical_columns:
        if bmrs_df[col].isnull().sum() > 0:
            bmrs_df[col].fillna(bmrs_df[col].mean(), inplace=True)
            print(f"Inputted missing values in column '{col}' with mean value.")
    duplicates = bmrs_df.duplicated().sum()
    if duplicates > 0:
        bmrs_df.drop_duplicates(inplace=True)
        print(f"Removed{duplicates} duplicated rows from data. ")
    return bmrs_df
print(bmrs_df.head())

def joined_data(bmrs_df, linear_order_json):
    # Extract 'records' from the JSON and normalize into a DataFrame
    records = linear_order_json.get('result', {}).get('records', [])
    linear_order_df = pd.json_normalize(records)
    # Check if 'DeliveryStart' exists in the extracted DataFrame.
    # If it doesn't, fall back to using 'startTimeOfHalfHrPeriod' if available.
    if 'DeliveryStart' in linear_order_df.columns:
        linear_order_df['DeliveryStart'] = pd.to_datetime(linear_order_df['DeliveryStart'])
    elif 'startTimeOfHalfHrPeriod' in linear_order_df.columns:
        linear_order_df['DeliveryStart'] = pd.to_datetime(linear_order_df['startTimeOfHalfHrPeriod'])
    else:
        raise ValueError("Neither 'DeliveryStart' nor 'startTimeOfHalfHrPeriod' is available in the JSON structure for joining.")
    bmrs_df['startTimeOfHalfHrPeriod'] = pd.to_datetime(bmrs_df['startTimeOfHalfHrPeriod'],format='%d/%m/%Y' )
    linear_order_df['DeliveryStart'] = pd.to_datetime(linear_order_df['DeliveryStart'])
    joined_data = pd.merge(
        bmrs_df,
        linear_order_df,
        left_on='startTimeOfHalfHrPeriod',
        right_on='DeliveryStart',
        how='inner'
    )
    return joined_data

joined_df = joined_data(bmrs_df, linear_order_df)   

def feature_engineering(df):
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pandas DataFrame")
    
    print("Columns in the DataFrame:", df.columns.tolist())
    print("Data types of columns:", df.dtypes)
    
    # Check for the correct datetime column
    if 'DeliveryStart' in df.columns:
        time_column = 'DeliveryStart'
    elif 'startTimeOfHalfHrPeriod' in df.columns:
        time_column = 'startTimeOfHalfHrPeriod'
    else:
        raise ValueError("Neither 'DeliveryStart' nor 'startTimeOfHalfHrPeriod' found in the DataFrame")

    print(f"Using time column: {time_column}")
    print(f"Sample of {time_column}:", df[time_column].head())

    # Ensure the time column is in datetime format
    df[time_column] = pd.to_datetime(df[time_column])
    print(f"{time_column} dtype after conversion:", df[time_column].dtype)

    # Sort by the time column for rolling calculations
    df = df.sort_values(by=time_column)

    # Check if required columns exist
    required_columns = ['initialForecastSpnGeneration', 'ExecutedVolume']
    for col in required_columns:
        if col not in df.columns:
            print(f"Warning: '{col}' not found in DataFrame")

    # Calculate 6-hour rolling median for selected columns
    if 'initialForecastSpnGeneration' in df.columns:
        try:
            df['RollingMedian_initialForecastSpnGeneration'] = (
                df['initialForecastSpnGeneration'].rolling('6H', on=time_column).median()
            )
        except Exception as e:
            print(f"Error calculating rolling median for initialForecastSpnGeneration: {str(e)}")
    
    if 'ExecutedVolume' in df.columns:
        try:
            df['RollingMedian_ExecutedVolume'] = (
                df['ExecutedVolume'].rolling('6H', on=time_column).median()
            )
        except Exception as e:
            print(f"Error calculating rolling median for ExecutedVolume: {str(e)}")

    # Aggregate data to daily view
    agg_columns = {}
    if 'initialForecastSpnGeneration' in df.columns:
        agg_columns['initialForecastSpnGeneration'] = 'sum'
    if 'ExecutedVolume' in df.columns:
        agg_columns['ExecutedVolume'] = 'sum'
    
    daily_aggregate = df.resample('D', on=time_column).agg(agg_columns).reset_index()

    # Aggregate data to weekly view
    weekly_aggregate = df.resample('W', on=time_column).agg(agg_columns).reset_index()

    return df, daily_aggregate, weekly_aggregate


# check_data_quality(bmrs_df)
# transform_data(bmrs_df)
# joined_data(bmrs_df, linear_order_df)
# feature_engineering(joined_df)
