from datetime import datetime
from dotenv import dotenv_values
dotenv_values()

def get_api_credentials():
    '''
    This function retrieves api credential/secrets from an environment variable file (.env) 
    Parameter: Takes no parameter
    Return value: a tuple containing api credentials
    Return type: tupple
    '''
    # Get API credentials from environment variable file withing the project directory
    config = dict(dotenv_values('.env'))
    account_id = config.get('ACCOUNT_ID')
    api_key = config.get('API_KEY')
    return account_id, api_key


def check_last_updated(json_record, response_data):
    '''
    This function retrieve the last time data was updated from the JSON file and
    check if the timestamp of the newly pulled data is not same as the last updated timestamp 
    Parameters: 
    - json_record : Records from the external JSON file
    - response_data : Record of the newly pulled data
    Return value: True or False 
    Return type: Boolean
    '''
    # Retrieve the last time (timestamp) data was updated from the JSON file
    last_updated = datetime.strptime(json_record[-1].get('timestamp'), '%Y-%m-%dT%H:%M:%SZ').date()
    # Check if the timestamp of the newly pulled data is not same as the last_updated above
    if (datetime.strptime(response_data.get('timestamp'), '%Y-%m-%dT%H:%M:%SZ').date()) != last_updated:
        return True
    else:
        return False
