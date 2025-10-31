"""
Traditional ETL orchestration (without DLT)
Use this only for testing individual components
"""

import logging
from extract import extract_data
from transform import transform_data
from load import load_to_duckdb

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_etl():
    """ETL flow without DLT orchestration"""
    logger.info("Running ETL")
    
    raw_data = extract_data(save_raw=True)
    logger.info(f"Extracted {len(raw_data)} records")
    
    df_clean = transform_data(raw_data)
    logger.info(f"Transformed to {len(df_clean)} records")
    
    load_to_duckdb(df_clean, overwrite=False)
    logger.info("Load complete")

if __name__ == "__main__":
    run_etl()