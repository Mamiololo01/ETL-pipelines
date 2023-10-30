import pandas as pd
import os
import requests
from bs4 import BeautifulSoup as bs
from sqlalchemy import create_engine
from datetime import datetime


# Database credentials
db_user_name = 'chris'
db_password = 'admin1234'
host = 'localhost'
port = 5432
db_name = 'security_data_db'



main_url = 'https://afx.kwayisi.org/ngx/'
list_of_df = []

# Data Extraction layer
def extract_data():
    for page in range(1, 3):
        url = main_url + f'?page={page}'  # Modify the URL for each page
        scrapped_data = requests.get(url)
        scrapped_data = scrapped_data.content
        soup = bs(scrapped_data, 'lxml')
        html_data = str(soup.find_all('table')[3])
        df = pd.read_html(html_data)[0]
        list_of_df.append(df)
    combined_data = pd.concat(list_of_df)
    combined_data.to_csv('data/raw_jazzy_investment.csv', index= False)
    print('Data Successfully written to a csv file')

def transform():
    stock_data = pd.read_csv('data/raw_jazzy_investment.csv') # Read csv file
    #Add a new column with the current timestamp when the scrapped is made
    current_date = datetime.today().strftime("%Y-%m-%d")
    stock_data['Date'] = current_date
    # Re-arrange columns
    stock_data = stock_data[['Date', 'Ticker', 'Name', 'Volume', 'Price', 'Change']]
    stock_data.to_csv('data/trans_jazzy_investment.csv', index= False)
    print('Data transformed and written to a csv file')

# Data Extraction layer
# def extract_data():
#     data = pd.DataFrame()
#     url = 'https://afx.kwayisi.org/ngx/'
#     #url2 = 'https://afx.kwayisi.org/ngx/?page=2'
#     scrapped_data = requests.get(url)
#     scrapped_data = scrapped_data.content
#     soup = BeautifulSoup(scrapped_data, 'lxml') # Parser
#     html_data = str(soup.find_all('table')[3])
#     df = pd.read_html(html_data)[0]
#     df.to_csv('data/raw_jazzy_investment.csv', index= False)
#     print('Data Successfully written to a csv file')


# Data load transformation layer
# def transform_data():
#     data = pd.read_csv('data/raw_jazzy_investment.csv') # Read csv file
#     def get_volume(Volume):
#         if Volume == '':
#             return (Volume).mean()
#         else:
#             return (Volume)
#     data['Volume'] = data['Volume'].apply(get_volume)
#     data.insert(['Date'])
#     data = data[['Ticker', 'Name', 'Volume', 'Price', 'Change', 'Date']]
#     data.to_csv('data/trans_jazzy_investment.csv', index= False)
#     print('Data transformed and written to a csv file')

#Data loading layer
def load_to_db():
    data = pd.read_csv('data/trans_jazzy_investment.csv') # Read csv file
    engine = create_engine(f'postgresql+psycopg2://{db_user_name}:{db_password}@{host}:{port}/{db_name}')
    data.to_sql('stock_rate', con= engine, if_exists='append', index= False)
    print('Data successfully written to PostgreSQL database')


#extract_data()
load_to_db()
#transform()