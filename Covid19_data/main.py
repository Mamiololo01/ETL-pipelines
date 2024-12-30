import os
import gdown
from datetime import datetime
#import re
import pandas as pd
from sqlalchemy import create_engine
import ast # Abstract syntax tree used for obtaining actual python objects from string
from dotenv import dotenv_values
dotenv_values()

# Database credentials
db_user_name = 'postgres'
db_password = 'postgres'
host = 'localhost'
port = 5432
db_name = 'covid19_db'

# def get_database_conn():
#     # Get database credentials from environment variable
#     config = dict(dotenv_values('.env'))
#     db_user_name = config.get('db_username')
#     db_password = config.get('db_password')
#     db_name = config.get('db_name')
#     port = config.get('port')
#     host = config.get('host')
#     # Create and return a postgresql database connection object
#     return create_engine(f'postgresql+psycopg2://{db_user_name}:{db_password}@{host}:{port}/{db_name}')

###  Data Extraction layer
def extract_data():
    file_url = 'https://drive.google.com/uc?id=1SzmRIwlpL5PrFuaUe_1TAcMV0HYHMD_b'
    # Output file path
    output_file = 'extract/Covid_19_data.csv'
    # Download the file
    gdown.download(file_url, output_file, quiet=False)
    print(f'The file {output_file} has been downloaded successfully.')

    
    # covid_extract_data = []
    # #Declares the json's dictionary confirmed 12 keys for locating values
    # data_keys = ["SNo", "ObservationDate", "Province", "Country", "LastUpdate", "Confirmed", "Death", "Recovered"]
    # # with open('Covid19_data/covid_19_data.csv', 'r') as covid19_file:
    #     # for covid_data in covid19_file:
    #     #     log_data = ast.literal_eval(log_data) # use for converting string to valid dict/lis/tuple etc
    #     #     log_date = log_data[log_keys[0]]
    #     #     event_date = datetime.strptime(log_date, '%Y%m%d')
    #     #     user_id = log_data[log_keys[5]]
    #     #     event_name = log_data[log_keys[2]]
    #     #     mobile_os_hardware_model = log_data[log_keys[8]]
    #     #     version = log_data[log_keys[11]]
    #     #     log_extract_data.append([event_date, user_id, event_name, mobile_os_hardware_model, version])
    # #Copy extracted data to a pandas dataframe
    # # covid19_data = pd.DataFrame(covid_extract_data, columns=["SNo", "ObservationDate", "Province", "Country", "LastUpdate", "Confirmed", "Death", "Recovered"])
    # # Export data to a CSV file. This is your stanging area
    # filename = f'covid_extract_data _{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'
    # path = f'raw/{filename}'
    # if os.path.exists('extract') == False:
    #     os.mkdir('extract')
    #     covid_extract_data .to_csv(path, index= False) 
    #     print('Data extracted and written to file')
    # else:
    #     covid_extract_data .to_csv(path, index= False)
    #     print('Data extracted and written to file')


# Data transformation layer 
def transform_data():

# Replace 'your_dataset.csv' with the actual path to your CSV file
    df = pd.read_csv('extract/Covid_19_data.csv')
   # Display the initial data types of the columns
    print("Initial Data Types:")
    print(df.dtypes)
   # Convert 'ObservationDate' column to datetime data type from string
    df['ObservationDate'] = pd.to_datetime(df['ObservationDate'])
   # Display the data types after the conversion
    print("\nData Types After Conversion:")
    print(df.dtypes)
   # Replace 'output_dataset.csv' with the desired output file path
    df.to_csv('transformed/Covid_19_data.csv', index=False)

    print("Transformed data is now written to a csv file")

    # # Define a function to convert the ObservationDate column to a uniform date format
    # def convert_to_date(date_string):
    #     date_value = datetime.strptime(date_string, '%m-%d-%Y').date()
    #     return date_value
    # files = os.listdir('extract')
    # for file in files:
    #     Covid_19_data = pd.read_csv(f'extract/{file}')
    #     Covid_19_data['Date'] = Covid_19_data['ObservationDate'].apply(convert_to_date)
    #     Covid_19_data.to_csv(f'transformed/{file}', index=False)
    # print('Transformed data is now written to a csv file')

    

# def load_data():
#     db_connection = get_database_conn()
#     csv_files = os.listdir('transformed')
#     for file in csv_files:
#         covid19_data = pd.read_csv(f'transformed/{file}')
#         covid19_data.to_sql('log_data', con= db_connection, if_exists= 'append', index=False)
#     print('Data loaded succesfully in postgress db')
    
#Data loading layer
def load_to_db():
    data = pd.read_csv('transformed/Covid_19_data.csv') # Read csv file
    engine = create_engine(f'postgresql+psycopg2://{db_user_name}:{db_password}@{host}:{port}/{db_name}')
    data.to_sql('covid19_data', con= engine, if_exists='append', index= False)
    print('Data successfully written to PostgreSQL database')


load_to_db()
#transform_data()
#extract_data()
#load_data()