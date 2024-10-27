import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

create_transformed_table = '''
CREATE TABLE IF NOT EXISTS transformed_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    DeliveryStart TIMESTAMP,
    initialForecastSpnGeneration FLOAT,
    ExecutedVolume FLOAT,
    RollingMedian_initialForecastSpnGeneration FLOAT,
    RollingMedian_ExecutedVolume FLOAT
);
'''

create_daily_aggregate_table = '''
CREATE TABLE IF NOT EXISTS daily_aggregate (
    id INT AUTO_INCREMENT PRIMARY KEY,
    DeliveryStart DATE,
    total_initialForecastSpnGeneration FLOAT,
    total_ExecutedVolume FLOAT
);
'''

create_weekly_aggregate_table = '''
CREATE TABLE IF NOT EXISTS weekly_aggregate (
    id INT AUTO_INCREMENT PRIMARY KEY,
    DeliveryStart DATE,
    total_initialForecastSpnGeneration FLOAT,
    total_ExecutedVolume FLOAT
);
'''

# Define your database configuration using environment variables
db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME')
}

# Apply schema to a MySQL database
def apply_schema_to_db():
    conn = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        cursor.execute(create_transformed_table)
        cursor.execute(create_daily_aggregate_table)
        cursor.execute(create_weekly_aggregate_table)

        conn.commit()
        print("Schema applied successfully")

    except mysql.connector.Error as e:
        print(f"Error: {e}")

    finally:
        if conn is not None and conn.is_connected():
            conn.close()

# Call the function to apply the schema
apply_schema_to_db()

# Function to insert data into the database
def insert_data_to_db(df, table_name):
    try:
        conn = mysql.connector.connect(
            host='localhost',  
            user='mysql',  
            password='admin@123', 
            database='forecasting_data'  
        )
        cursor = conn.cursor()

        # Create a list of tuples from the dataframe
        data = [tuple(row) for row in df.to_numpy()]

        # Prepare the SQL query
        placeholders = ', '.join(['%s'] * len(df.columns))
        columns = ', '.join(df.columns)
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        # Execute the query
        cursor.executemany(sql, data)

        # Commit the changes
        conn.commit()
        print(f"Data inserted successfully into {table_name}")

    except Error as e:
        print(f"Error: {e}")

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
