"""
Manual DuckDB loader for testing/debugging purposes.
"""
import duckdb
import polars as pl
from datetime import datetime
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_PATH = "data/output.duckdb"
TABLE_NAME = "github_repos_snapshot"

def load_to_duckdb(df: pl.DataFrame, overwrite: bool = False) -> None:
    """
    Manual loader for testing/debugging.
    
    Args:
        df: Polars DataFrame to load
        overwrite: If True, replace existing table; if False, append
        
    """
    if df.is_empty():
        logger.warning("No data to load into DuckDB.")
        return

    # Ensure data directory exists
    Path("data").mkdir(exist_ok=True)
    
    # Add snapshot date
    snapshot_date = datetime.utcnow().strftime("%Y-%m-%d")
    df = df.with_columns(pl.lit(snapshot_date).alias("snapshot_date"))

    try:
        con = duckdb.connect(DB_PATH)

        if overwrite:
            logger.info(f"Overwriting table: {TABLE_NAME}")
            con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
            con.execute(f"CREATE TABLE {TABLE_NAME} AS SELECT * FROM df")
        else:
            logger.info(f"Appending to table: {TABLE_NAME}")
            con.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} AS SELECT * FROM df WHERE FALSE")
            con.execute(f"INSERT INTO {TABLE_NAME} SELECT * FROM df")

        logger.info(f"Loaded {df.shape[0]} rows into DuckDB.")
        
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise
    finally:
        con.close()