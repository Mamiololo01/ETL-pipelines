import pandas as pd
import requests
import json
import os
from datetime import datetime
from util import check_last_updated


def get_exchange_rates(account_id, api_key):
    '''
    This function sends request and retrieve exchnage rate data for all the specified countries from XE REST API and
    write the data to an external JSON file. 
    Parameter: Takes in two parameters - XE account ID and API key
    Return value: None. Does not return a value
    Return type: None
    '''
    #to_cur = ["NGN", "GHS", "KSH", "USH", "MAD", "CFA", "EGP"]
    url = 'https://xecdapi.xe.com/v1/convert_from.json/?from=USD&to=NGN,GHS,KES,UGX,MAD,EGP,XAF&amount=1'
    
    try: # A try-except block to handle error authenticating or retrieving data from the API
        response = requests.get(url=url, auth= (account_id, api_key))
        response_data = response.json()
        try: # A try-except block to handle error due to reading or writing to the data directory.
            if os.path.exists('./raw/exchange_rates_data.json'): # Check if the raw exchange rate data exists
                with open('raw/exchange_rates_data.json', 'r+') as xrates_file: # Open file in both read and write mode
                    records = json.load(xrates_file) # Read data from external JSON file
                    if isinstance(records, dict):
                        updated_records = [records]
                    else:
                        updated_records = records
                    # The check_last_updated function is first used to check if data for the current run date already exist.
                    #The update and write operation is skipped if this is true
                    if check_last_updated(updated_records, response_data):
                        updated_records.append(response_data) # update existing data with new data
                        xrates_file.seek(0) # Reset to start of file to overite old data
                        xrates_file.write('[ \n')
                        for index, item in enumerate(updated_records): 
                            if index != (len(updated_records) -1): # checks if the current iteration is not the last iteration
                                json.dump(item, xrates_file) # Write updated data to an external JSON file
                                xrates_file.write(',\n')
                            else: # If the current iteration is the last iteration then a comma symbol is not add to end of line
                                json.dump(item, xrates_file)
                                xrates_file.write('\n')
                        xrates_file.write(']')
                        print('exchange rate data successfully updated')
                    # This block skips writing data to the external JSON file
                    else:
                        print('Data for today already exists!')
            else:
                with open('raw/exchange_rates_data.json', 'w') as xrates_file: # First time data is written to file
                    json.dump(response_data, xrates_file) # Write pulled data to an external JSON file
                    print('exchange rate data successfully written to a JSON file')
        except FileExistsError:
            print('Directory does not exist')
    except requests.exceptions.RequestException as err:
        print(f'Unable to get data from API. Error: {err}')

def transform_data():
    '''
    This function reads exchanges rates data from an external JSON file in the raw directory,
    perform some transformations on the data to put in a structured and desired format and 
    write the final output into an external csv file.
    Parameters: Takes no parameter
    Return value: Does not return a value
    Return type: None
    '''
    with open('raw/exchange_rates_data.json', 'r+') as xrates_file:
        records = json.load(xrates_file)
        if isinstance(records, dict):
            records = [records]
        rates_data = pd.DataFrame(records)[['timestamp', 'from', 'to', 'amount']]
        
        #Rename columns
        rates_data.rename(columns= {'amount': 'usd_to_currency_rate', 'from': 'currency_from'}, inplace= True)
        # Transform column data
        rates_data['currency_to'] = rates_data['to'].apply(lambda x: [item['quotecurrency'] for item in x])
        rates_data['currency_to_usd_rate'] = rates_data['to'].apply(lambda x: [item['mid'] for item in x])
        rates_data['timestamp'] = rates_data['timestamp'].apply(lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').date())
        #Retrieve all currencies
        currencies = [item.get('quotecurrency') for item in records[0].get('to')]
        country_xchange_rates_df = []
        for i in range(len(currencies)):  #['GHS', 'NGN', 'KES'] [0, 1, 2, 3, 4, 5]
            country_xchange_rates = rates_data[rates_data.columns] # Create a copy of rates_data dataframe
            country_xchange_rates['currency_to'] = country_xchange_rates['currency_to'].apply(lambda x: x[i]) #Extract country currency name into a column 
            country_xchange_rates['currency_to_usd_rate'] = country_xchange_rates['currency_to_usd_rate'].apply(lambda x: x[i]) #Extract country exchange rate value into a column 
            country_xchange_rates = country_xchange_rates[['timestamp', 'currency_from', 'usd_to_currency_rate', 'currency_to_usd_rate', 'currency_to']]
            country_xchange_rates_df.append(country_xchange_rates) # Aggregate currency data into a dataframe
            # Write specific country exchange rate data to an external file
            country_xchange_rates.to_csv(f'transformed/USD_to_{currencies[i]}_rate_conversion.csv', index=False)
        # Combine all the country exchange rates data
        country_xchange_rates = pd.concat(country_xchange_rates_df)
        # Write transformed data to an external csv file
        country_xchange_rates.to_csv('transformed/combined_rates_conversion.csv', index=False)




# Data Loading Layer
def load_to_db():
    db_connection = get_database_conn()
    df1 = pd.read_csv('transformed/USD_to_EGP_rate_conversion.csv')
    df2 = pd.read_csv('transformed/USD_to_GHS_rate_conversion.csv')
    df3 = pd.read_csv('transformed/USD_to_MAD_rate_conversion.csv')
    df4 = pd.read_csv('transformed/USD_to_NGN_rate_conversion.csv')

    comb_xchange_data = pd.concat([df1, df2, df3, df4], axis=0)
    comb_xchange_data.to_sql('comb_xchange_data', con= db_connection, if_exists= 'append', index=False)


#transform_data()
#load_to_db()

