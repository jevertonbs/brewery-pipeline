import pandas as pd
import requests
import json
import time
from loguru import logger
import os
from typing import Optional

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# Configure logger
logger.remove()  # Remove default handlers
logger.add("bronze_layer.log", rotation="1 week", retention="30 days", compression="zip")  # Rotating logs


def fetch_data_from_api(url: str, max_attempts: int = 10) -> Optional[list]:
    """
    Fetches data from the API with retry logic.
    
    Args:
        url (str): API endpoint.
        max_attempts (int): Maximum number of retry attempts.
    
    Returns:
        Optional[list]: Parsed JSON data from the API, or None if all attempts fail.
    """
    attempt = 0
    while attempt < max_attempts:
        try:
            logger.info(f"Attempt {attempt + 1}/{max_attempts} to fetch data from {url}")
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            logger.info("Data fetched successfully.")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt + 1} failed: {e}")
            attempt += 1
            time.sleep(2 ** attempt)  # Exponential backoff
    logger.error(f"Failed to fetch data from {url} after {max_attempts} attempts.")
    return None


def save_data_to_file(data: list, file_path: str) -> None:
    """
    Saves data to a JSON file.
    
    Args:
        data (list): Data to save.
        file_path (str): Path to the output JSON file.
    """
    try:
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)
        logger.info(f"Data successfully saved to {file_path}.")
    except IOError as e:
        logger.error(f"Error saving data to {file_path}: {e}")
        raise


def load_data_to_dataframe(data: list) -> pd.DataFrame:
    """
    Loads data into a Pandas DataFrame.
    
    Args:
        data (list): Data to load.
    
    Returns:
        pd.DataFrame: DataFrame containing the data.
    """
    try:
        df = pd.DataFrame(data)
        if df.empty:
            raise ValueError("The loaded data is empty.")
        logger.info(f"Data loaded into DataFrame with {len(df)} records.")
        return df
    except Exception as e:
        logger.error(f"Error loading data into DataFrame: {e}")
        raise


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Loads data from the Open Brewery DB API and returns a Pandas DataFrame.
    Saves the raw data as a JSON file named bronze_layer.json.
    """
    url = 'https://api.openbrewerydb.org/breweries'
    bronze_file_path = 'bronze_layer.json'

    data = fetch_data_from_api(url)
    if data is None:
        return pd.DataFrame()  # Return empty DataFrame if fetching data fails

    save_data_to_file(data, bronze_file_path)
    return load_data_to_dataframe(data)


@test
def test_output(output, *args) -> None:
    """
    Validates the output DataFrame from the Bronze block.
    """
    logger.info("Starting Bronze block output validation.")
    assert isinstance(output, pd.DataFrame), "Output is not a DataFrame."
    assert not output.empty, "The output DataFrame is empty."

    expected_columns = ['id', 'name', 'brewery_type', 'city', 'state']
    missing_columns = [col for col in expected_columns if col not in output.columns]
    assert not missing_columns, f"Missing columns in the DataFrame: {missing_columns}"

    assert output['id'].is_unique, "Duplicate IDs found in the DataFrame."
    logger.info("Bronze block output validation completed successfully.")


@test
def test_file_creation(*args, **kwargs) -> None:
    """
    Verifies the existence of the bronze_layer.json file.
    """
    bronze_file_path = 'bronze_layer.json'
    assert os.path.exists(bronze_file_path), f"The file {bronze_file_path} was not created."
    logger.info(f"The file {bronze_file_path} exists and is validated.")
