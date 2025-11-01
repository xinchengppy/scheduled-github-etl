# GitHub repos ETL with DLT, Polars & DuckDB

A lightweight **data engineering pipeline** that extracts repository metadata from the GitHub REST API, transforms it with **Polars**, and loads it into a local **DuckDB** database — fully automated with **GitHub Actions**.

---

## Overview

This project demonstrates a complete **DE workflow**:

1. **Extract** – Fetch GitHub organization repositories using the REST API (v3) - I use 'apache org' here
2. **Transform** – Clean, normalize, and enrich data using Polars  
3. **Load** – Store results into a DuckDB database for analytical queries  
4. **Automate** – Schedule daily runs with GitHub Actions  
5. **Test** – Validate transformations via `pytest` in CI  

---

## Architecture

```text
github-etl/
├── src/
│   ├── extract.py         # GitHub API client
│   ├── transform.py       # Polars transformation logic
│   ├── load.py            # Write to DuckDB (for local testing)
│   └── main.py            # Local ETL entrypoint
├── dlt/
│   └── pipeline.py        # DLT pipeline definition
├── config/
│   └── config.toml        # GitHub token, orgs, output path
├── tests/
│   └── test_transform.py  # Unit tests for transformations
├── data/
│   └── output.duckdb      # DuckDB output (created at runtime)
├── .github/
│   └── workflows/
│       └── schedule.yml   # Daily ETL + tests workflow
├── requirements.txt
└── README.md
```


## Configuration
Edit `config/config.toml`:

```toml
[github]
token = "ghp_your_token_here"
orgs = ["<any orgs you want>"]

[output]
db_path = "data/output.duckdb"
mode = "merge"
```

Or use an environment variable:
```bash
export GITHUB_TOKEN=ghp_your_token_here
```

## If you want to run locally
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# Run the ETL pipeline
python3 dlt/pipeline.py
# Run tests
python3 -m pytest -v
```



## GitHub Actions
Daily scheduled run (0 2 * * *) defined in `.github/workflows/schedule.yml`

1. Runs tests
2. Executes ETL
3. Uploads output.duckdb as an artifact

## Example Output
| full_name         | stars  | forks  | language | days_since_last_push | star_fork_ratio | snapshot_date |
|-------------------|--------|--------|-----------|-----------------------|-----------------|----------------|
| apache/airflow    | 33000  | 13400  | Python    | 2                     | 2.46            | 2025-11-01     |
| apache/spark      | 38900  | 30000  | Scala     | 3                     | 1.29            | 2025-11-01     |
| apache/kafka      | 28000  | 14000  | Java      | 4                     | 2.00            | 2025-11-01     |
| apache/flink      | 22000  | 12000  | Java      | 7                     | 1.83            | 2025-11-01     |
| apache/hive       | 4800   | 4100   | Java      | 12                    | 1.17            | 2025-11-01     |


## Acknowledgments
This project was for personnal learning purpose, guided by [Dataskew.io](https://github.com/dataskew-io).
Huge thanks to the Data Engineering Projects provided by [Dataskew.io](https://dataskew.io/dashboard/) and [Adriano Sanges](https://www.linkedin.com/in/adrianosanges/)!
