from mage_ai.io.file import FileIO
from pandas import DataFrame
from loguru import logger
import pandas as pd
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(data, *args, **kwargs):
    """
    Prepares the data from the Silver Layer for analysis by saving it in Parquet format.
    Applies aggregations and calculations if necessary.

    Args:
        data: The data from the Silver Layer that comes from the previous block
        args: Other optional arguments
        kwargs: Additional parameters

    Output (optional):
        Returns any object, and it will be logged and displayed during block execution
    """
    try:
        logger.info("Transforming data for the Gold Layer...")

        # Checking for missing values in important columns
        if data[['brewery_state', 'brewery_id', 'type']].isnull().any().any():
            logger.warning("There are missing values in the columns 'brewery_state', 'brewery_id', or 'type'. Please correct the data before proceeding.")
            raise ValueError("Missing data in required columns for aggregation.")

        # Calculating metrics in the Gold Layer: number of breweries by type and location
        df_gold = data.groupby(['brewery_state', 'type']).agg(
            total_breweries=('brewery_id', 'count')
        ).reset_index()

        # Checking the first rows after transformation
        logger.info(f"First rows of the Gold Layer data:\n{df_gold.head()}")

        # Creating directory to save the data, if it doesn't exist
        output_dir = 'gold_layer'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Saving the data in Parquet format
        gold_file_path = f'{output_dir}/breweries_gold.parquet'
        logger.info(f"Saving the data in the Gold Layer to {gold_file_path}...")

        # If the data volume is large, consider partitioning by state
        try:
            df_gold.to_parquet(gold_file_path, index=False)
            logger.info("Gold Layer data saved successfully!")
        except Exception as e:
            logger.error(f"Error saving data in Parquet format: {e}")
            # Fallback to saving in CSV if Parquet fails
            csv_file_path = f'{output_dir}/breweries_gold.csv'
            df_gold.to_csv(csv_file_path, index=False)
            logger.info(f"Data saved as CSV instead of Parquet at {csv_file_path}.")
            raise

        # Verifying if the Parquet file is saved correctly
        verify_parquet(gold_file_path)

        return df_gold

    except ValueError as ve:
        logger.error(f"Data error: {ve}")
        raise
    except Exception as e:
        logger.exception(f"Error creating the Gold Layer: {e}")
        raise


def verify_parquet(file_path):
    """
    Verifies if the Parquet file exists and can be loaded correctly.

    Args:
        file_path: The path of the Parquet file to be verified.

    Raises:
        Exception: If the Parquet file doesn't exist or cannot be loaded.
    """
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Parquet file {file_path} not found.")

        # Try to read the Parquet file
        pd.read_parquet(file_path)
        logger.info(f"Parquet file {file_path} is valid and loaded successfully.")

    except Exception as e:
        logger.error(f"Error verifying Parquet file: {e}")
        raise
