import pandas as pd
import os
from main import transform_data, feature_engineering, apply_schema_to_db

def test_clean_data():
    # Sample DataFrame with null values
    df = pd.DataFrame({
        'A': [1, 2, None, 4],
        'B': [None, 2, 3, 4]
    })
    cleaned_df = clean_data(df)

    # Check if null values are replaced
    assert cleaned_df.isnull().sum().sum() == 0

def test_feature_engineering():
    # Sample DataFrame with datetime and numerical data
    df = pd.DataFrame({
        'DeliveryStart': pd.date_range(start='2024-01-01', periods=10, freq='H'),
        'initialForecastSpnGeneration': range(10),
        'ExecutedVolume': range(10)
    })
    transformed_df, daily_agg, weekly_agg = feature_engineering(df)

    # Test if rolling median columns exist
    assert 'RollingMedian_initialForecastSpnGeneration' in transformed_df.columns
    assert 'RollingMedian_ExecutedVolume' in transformed_df.columns



