import polars as pl
from datetime import datetime, timezone
import logging
from typing import List, Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transform_data(raw_data: List[Dict]) -> pl.DataFrame:
    """Transform extracted GitHub data with Polars."""
    if not raw_data:
        logger.warning("No raw data to transform.")
        return pl.DataFrame()

    today = datetime.now(timezone.utc)
    
    # Select relevant columns
    cols = [
        "name",
        "full_name",
        "stars",
        "forks",
        "license",
        "language",
        "created_at",
        "updated_at",
        "pushed_at"
    ]
    df = pl.DataFrame(raw_data).select(cols)

    # Convert date columns with error tracking
    date_columns = ["created_at", "updated_at", "pushed_at"]
    for col in date_columns:
        df = df.with_columns([
            pl.col(col).str.to_datetime(format="%Y-%m-%dT%H:%M:%SZ", strict=False)
            .dt.replace_time_zone("UTC")
            .alias(col)
        ])
        df = df.with_columns(
            pl.col(col).is_null().alias(f"{col}_is_invalid")
        )
        invalid_count = df.filter(pl.col(f"{col}_is_invalid")).shape[0]
        if invalid_count > 0:
            logger.warning(f"Found {invalid_count} invalid dates in {col}")

    # Calculate days since last push with null handling
    df = df.with_columns([
        (pl.lit(today) - pl.col("pushed_at"))
        .dt.total_days()
        .alias("days_since_last_push")
    ])

    # Calculate star-fork ratio with proper handling of zeros and nulls
    df = df.with_columns([
        (pl.when(pl.col("forks") > 0)
         .then(pl.col("stars") / pl.col("forks").cast(pl.Float64))
         .otherwise(0.0)
         ).alias("star_fork_ratio")
    ])

    # Fill nulls appropriately by column type
    df = df.with_columns([
        pl.col("license").fill_null("unknown"),
        pl.col("language").fill_null("unknown"),
        pl.col("star_fork_ratio").fill_null(0.0),
        pl.col("days_since_last_push").fill_null(float('inf'))  # Ensure inactive repos get filtered
    ])

    # Filter inactive repositories (no pushes in last year)
    original_count = df.shape[0]
    df = df.filter(pl.col("days_since_last_push") < 365)
    filtered_count = original_count - df.shape[0]
    if filtered_count > 0:
        logger.info(f"Filtered out {filtered_count} inactive repositories")

    logger.info(f"Transformed {df.shape[0]} active repositories.")
    return df


# if __name__ == "__main__":
#     import json
#     from transform import transform_data

#     with open("data/raw_repos.json", "r") as f:
#         raw_data = json.load(f)

#     df_clean = transform_data(raw_data)