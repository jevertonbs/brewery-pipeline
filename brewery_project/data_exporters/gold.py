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
        df_gold.to_parquet(gold_file_path, index=False)

        logger.info("Gold Layer data saved successfully!")

        return df_gold

    except ValueError as ve:
        logger.error(f"Data error: {ve}")
        raise
    except Exception as e:
        logger.exception(f"Error creating the Gold Layer: {e}")
        raise
