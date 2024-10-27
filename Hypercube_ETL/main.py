#import libraries
import pandas as pd
import os
import mysql.connector
from etl import transform_data, joined_data, feature_engineering
from util import apply_schema_to_db, insert_data_to_db
from test import test_clean_data,  test_feature_engineering

# Function to insert data into the SQLite database
def insert_data_to_db(df, table_name, db_path='data/forecasting_data.db'):
    conn = mysql.connector.connect(db_config)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()