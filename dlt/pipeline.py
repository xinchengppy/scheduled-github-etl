import sys
from pathlib import Path
import logging
import dlt
from datetime import datetime, timezone
import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.extract import extract_data, load_config, CONFIG_PATH
from src.transform import transform_data

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration
config = load_config(CONFIG_PATH)
mode = config["output"].get("mode", "merge")
db_path = config['output']['db_path']

# Define DLT resource
@dlt.resource(name="github_repos", 
              write_disposition=mode, 
              primary_key=["full_name", "snapshot_date"])
def github_repos_resource():
    """DLT resource: extract → transform → yield clean rows."""
    logger.info("Starting GitHub data extraction...")
    raw_data = extract_data(save_raw=False)

    logger.info(f"Fetched {len(raw_data)} raw repositories. Starting transformation...")
    df = transform_data(raw_data)

    logger.info(f"Adding snapshot_date column...")
    snapshot_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    df = df.with_columns(pl.lit(snapshot_date).alias("snapshot_date"))

    logger.info(f"Transformation complete. Yielding {df.shape[0]} cleaned rows to DLT.")
    yield df.to_dicts()

# Define DLT pipeline
pipeline = dlt.pipeline(
    pipeline_name=f"github_etl_{datetime.now(timezone.utc).strftime('%Y_%m_%d')}",
    destination=dlt.destinations.duckdb(db_path),
    dataset_name="github_data",
    dev_mode=False
)

if __name__ == "__main__":
    try:
        load_info = pipeline.run(github_repos_resource())
        logger.info(f"Pipeline run completed successfully.")
    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
    finally:
        logger.info("Pipeline execution finished.\n")