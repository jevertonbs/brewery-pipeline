import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
from loguru import logger
import os

@transformer
def transform_data(*args, **kwargs):
    """
    Transforms the data from the Bronze Layer (JSON) into the Parquet or Delta format.
    Partitions the data by city/state and saves the result.
    """
    try:
        # Checks if the file exists before attempting to read
        bronze_file_path = 'bronze_layer.json'
        if not os.path.exists(bronze_file_path):
            raise FileNotFoundError(f"The file '{bronze_file_path}' was not found.")

        # Reading the JSON file from the Bronze Layer
        logger.info("Reading data from the Bronze Layer...")
        df = pd.read_json(bronze_file_path)

        # Checks if the data was loaded correctly
        if df.empty:
            raise ValueError("The data loaded from the Bronze Layer is empty.")

        # Cleaning or transforming the data (if necessary)
        logger.info("Performing transformations on the data...")

        # Renaming columns (example)
        df = df.rename(columns={
            'id': 'brewery_id',
            'name': 'brewery_name',
            'brewery_type': 'type',
            'city': 'brewery_city',
            'state': 'brewery_state',
        })

        # Dropping unnecessary columns (example)
        df = df.drop(columns=['address_2', 'address_3'], errors='ignore')

        # Checking the first few rows after transformation
        logger.info(f"First rows of transformed data:\n{df.head()}")

        # Checking the columns after renaming
        logger.info(f"Columns after transformation: {df.columns.tolist()}")

        # Partitioning the data by city/state
        output_dir = "silver_layer"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        logger.info("Saving transformed data in Parquet format...")

        # Saving in Parquet format with partition by state and city
        parquet_file_path = f'{output_dir}/breweries_silver.parquet'
        df.to_parquet(parquet_file_path, partition_cols=['brewery_state', 'brewery_city'])

        logger.info(f"Transformed data successfully saved to the file: {parquet_file_path}")
        
        return df
    except FileNotFoundError as fnf_error:
        logger.error(f"Error: {fnf_error}")
        raise
    except ValueError as val_error:
        logger.error(f"Value error: {val_error}")
        raise
    except Exception as e:
        logger.exception(f"Error while transforming the data: {e}")
        raise


@test
def test_parquet_creation(*args, **kwargs) -> None:
    """
    Tests if the Parquet file was created correctly in the Silver Layer.
    """
    try:
        parquet_file_path = 'silver_layer/breweries_silver.parquet'
        
        # Checks if the Parquet file was created
        assert os.path.exists(parquet_file_path), f"Parquet file '{parquet_file_path}' was not created."
        logger.info("Parquet file created successfully.")
        
        # Checks if the Parquet file is readable (optional)
        df = pd.read_parquet(parquet_file_path)
        assert not df.empty, "The Parquet file is empty."
        logger.info("Parquet file loaded and validated successfully.")
        
    except AssertionError as ae:
        logger.error(f"Test failed: {ae}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred in the test: {e}")
        raise
