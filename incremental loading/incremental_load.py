import os
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from dotenv import dotenv_values
dotenv_values()

def get_database_conn():
    # Get database credentials from environment variable
    config = dict(dotenv_values('.env'))
    db_user_name = config.get('db_username')
    db_password = config.get('db_password')
    db_name = config.get('db_name')
    port = config.get('port')
    host = config.get('host')
    # Create and return a postgresql database connection object
    return create_engine(f'postgresql+psycopg2://{db_user_name}:{db_password}@{host}:{port}/{db_name}')


connection = get_database_conn()

# Get new data
new_data = pd.read_csv(f'transformed/new_log_data.csv')
#date_convert = lambda date_val: datetime.strptime(date_val, '%d%m%Y')                 #
new_data['date'] = pd.to_datetime(new_data['date']).dt.date

# 2020-01-10 
query = '''
select max(date) from log_data   
'''

# # Get last time data was updated into the database
last_updated = pd.read_sql(query, con= connection).values[0][0]
new_data = new_data[new_data['date'] > last_updated]
new_data.to_sql('log_data', con= connection, if_exists = 'append', index = False)
print('Data loaded successfully')



