import pandas as pd
from dotenv import dotenv_values
import psycopg2
dotenv_values()

# Get credentials from environment variable file
config = dotenv_values('.env')

# Read Local CSV file
def read_local_csv(file_name):
    csv_data = pd.read_csv(file_name)
    return csv_data


def get_redshift_connection():
    #iam_role = config.get('IAM_ROLE')
    user = config.get('USER')
    password = config.get('PASSWORD')
    host = config.get('HOST')
    database_name = config.get('DATABASE_NAME')
    port = config.get('PORT')
    # create redshift connection usingf'dbname={database_name} host={host} port={port} user={user} password={password}')
    conn = psycopg2.connect(f'postgresql://{user}:{password}@{host}:{port}/{database_name}')
    return conn

def execute_sql(sql_query, conn):
    conn = get_redshift_connection()
    cur = conn.cursor() # Creating a cursor object for executing SQL query
    cur.execute(sql_query)
    conn.commit()
    cur.close() # Close cursor
    conn.close() # Close connection


def generate_schema(data, table_name = 'log_data'):
    create_table_statement = f'CREATE TABLE IF NOT EXISTS {table_name}(\n'
    column_type_query = ''
    
    types_checker = {
        'INT':pd.api.types.is_integer_dtype,
        'VARCHAR':pd.api.types.is_string_dtype,
        'FLOAT':pd.api.types.is_float_dtype,
        'TIMESTAMP':pd.api.types.is_datetime64_any_dtype,
        'OBJECT':pd.api.types.is_dict_like,
        'ARRAY':pd.api.types.is_list_like,
    }
    for column in data: # Iterate through all the columns in the dataframe
        last_column = list(data.columns)[-1] # Get the name of the last column
        for type_ in types_checker: 
            mapped = False
            if types_checker[type_](data[column]): # Check each column against data types in the type_checker dictionary
                mapped = True # A variable to store True of False if there's type is found. Will be used to raise an exception if type not found
                if column != last_column: # Check if the column we're checking its type is the last comlumn
                    column_type_query += f'{column} {type_},\n' # 
                else:
                    column_type_query += f'{column} {type_}\n'
                break
        if not mapped:
            raise ('Type not found')
    column_type_query += ');'
    output_query = create_table_statement + column_type_query
    return output_query


def load_to_redshift(table_name):
    s3_path = 's3://redshift-ololo-bucket' # Replace this with your file path (bucket name, folder & file name)
    iam_role = config.get('IAM_ROLE')
    conn = get_redshift_connection()
    # A copy query to copy csv files from S3 bucket to Redshift.
    copy_query = f"""
    copy {table_name}
    from '{s3_path}'
    IAM_ROLE '{iam_role}'
    csv
    IGNOREHEADER 1;
    """
    execute_sql(copy_query, conn)
    print('Data successfully loaded to Redshift')

