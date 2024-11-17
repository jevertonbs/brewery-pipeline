import pandas as pd
import requests
import json
import time
from loguru import logger
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


# Configuring the logger
logger.remove()  # Removes the default handlers
logger.add("bronze_layer.log", rotation="1 week", retention="30 days", compression="zip")  # Saves logs to a file


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Loads data from the Open Brewery DB API and returns a Pandas DataFrame.
    Saves the raw data as a JSON file named bronze_layer.json.
    Retries up to 10 times in case of request failure.
    """
    url = 'https://api.openbrewerydb.org/breweries' 
    
    max_attempts = 10  # Maximum number of attempts
    attempt = 0  # Attempt counter
    success = False  # Flag to check if the request was successful
    
    while attempt < max_attempts and not success:
        try:
            # Making the request to the API
            logger.info(f"Starting attempt {attempt + 1}/{max_attempts} to fetch data from the API.")
            response = requests.get(url)
            
            # Check if the request was successful
            response.raise_for_status()
            
            # If we are here, the request was successful
            success = True
            data = response.json()

            # Save the raw data in a JSON file
            with open('bronze_layer.json', 'w') as f:
                json.dump(data, f)

            # Convert the data to a DataFrame
            df = pd.DataFrame(data)
            
            logger.info(f"Data successfully loaded: {len(df)} records.")
            return df
        
        except requests.exceptions.RequestException as e:
            # Capture request errors (connection, timeout, etc)
            logger.error(f"Attempt {attempt + 1}/{max_attempts} failed: Error making the request: {e}")
            attempt += 1
            time.sleep(2)  # Wait 2 seconds before trying again
        except Exception as e:
            # Capture other types of errors
            logger.exception(f"Unexpected error while processing data: {e}")
            break  # Stop attempts on error

    # If it fails after all attempts, return an empty DataFrame
    if not success:
        logger.error(f"Failed to load data after {max_attempts} attempts.")
        return pd.DataFrame()


@test
def test_output(output, *args) -> None:
    """
    Tests the output of the Bronze block.
    Adds additional checks to ensure the data is correct.
    """
    logger.info("Starting validation of the Bronze block output.")

    assert output is not None, 'The output is undefined.'
    assert isinstance(output, pd.DataFrame), 'The output is not a DataFrame.'
    assert not output.empty, 'The DataFrame is empty.'

    # Check if some expected columns are present
    expected_columns = ['id', 'name', 'brewery_type', 'city', 'state']
    for column in expected_columns:
        assert column in output.columns, f'The column {column} is missing in the DataFrame.'
    
    logger.info("Column validation completed.")

    # Check if the DataFrame has more than 0 rows
    assert len(output) > 0, 'The DataFrame is empty or does not contain enough data.'

    # Check if all 'id' values are unique (optional)
    assert output['id'].is_unique, 'There are duplicate IDs in the DataFrame.'
    
    logger.info("Validation completed successfully.")

def test_file_creation(*args, **kwargs) -> None:
    """
    Tests if the `bronze_layer.json` file was created after loading the data.
    """
    import os
    assert os.path.exists('bronze_layer.json'), "The bronze_layer.json file was not created."

    logger.info("File bronze_layer.json generated successfully.")
