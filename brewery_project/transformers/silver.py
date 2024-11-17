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
    Includes robust error handling and logging.
    """
    try:
        bronze_file_path = 'bronze_layer.json'

        # Check if the file exists
        if not os.path.exists(bronze_file_path):
            raise FileNotFoundError(f"The file '{bronze_file_path}' was not found.")

        # Load JSON data
        logger.info("Reading data from the Bronze Layer...")
        df = pd.read_json(bronze_file_path)

        # Check for empty data
        if df.empty:
            raise ValueError("The data loaded from the Bronze Layer is empty.")

        # Perform transformations
        logger.info("Performing transformations on the data...")
        df = df.rename(columns={
            'id': 'brewery_id',
            'name': 'brewery_name',
            'brewery_type': 'type',
            'city': 'brewery_city',
            'state': 'brewery_state',
        })
        df = df.drop(columns=['address_2', 'address_3'], errors='ignore')

        # Data validation: Ensure required columns are present
        required_columns = ['brewery_id', 'brewery_name', 'type', 'brewery_city', 'brewery_state']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        logger.info(f"Transformed data preview:\n{df.head()}")

        # Partitioning the data
        output_dir = "silver_layer"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        logger.info("Saving transformed data in Parquet format...")
        parquet_file_path = f'{output_dir}/breweries_silver.parquet'
        df.to_parquet(parquet_file_path, partition_cols=['brewery_state', 'brewery_city'])

        logger.info(f"Transformed data saved successfully to: {parquet_file_path}")
        return df

    except FileNotFoundError as fnf_error:
        logger.error(f"File not found: {fnf_error}")
        raise
    except ValueError as val_error:
        logger.error(f"Value error: {val_error}")
        raise
    except pd.errors.ParserError as parser_error:
        logger.error(f"Data parsing error: {parser_error}")
        raise
    except Exception as e:
        logger.exception(f"Unexpected error during data transformation: {e}")
        raise


@test
def test_parquet_creation(*args, **kwargs) -> None:
    """
    Tests if the Parquet file was created correctly in the Silver Layer.
    """
    try:
        parquet_file_path = 'silver_layer/breweries_silver.parquet'

        # Check if the Parquet file exists
        assert os.path.exists(parquet_file_path), f"Parquet file '{parquet_file_path}' was not created."

        logger.info("Parquet file exists. Validating content...")

        # Load and validate Parquet data
        df = pd.read_parquet(parquet_file_path)
        assert not df.empty, "The Parquet file is empty."

        logger.info("Parquet file validation successful.")
        
    except AssertionError as ae:
        logger.error(f"Test failed: {ae}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during the test: {e}")
        raise
