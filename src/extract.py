import os
import time
import json
import requests
import tomli
from typing import List, Dict
from pathlib import Path
import logging

CONFIG_PATH = "config/config.toml"
DEFAULT_RAW_OUTPUT = "data/raw_repos.json"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_config(config_path=CONFIG_PATH) -> Dict:
    with open(config_path, "rb") as f:
        return tomli.load(f)

def get_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json"
    }

def fetch_all_repos(org: str, headers: Dict[str, str]) -> List[Dict]:
    logger.info(f"Fetching repos from {org}")
    url = f"https://api.github.com/orgs/{org}/repos"
    params = {"per_page": 100, "page": 1}
    all_repos = []

    while True:
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 403: # Rate limit exceeded
            reset_time = int(response.headers.get("X-RateLimit-Reset", time.time() + 60))
            wait_seconds = max(reset_time - int(time.time()), 1)
            logger.warning(f"Rate limited. Sleeping {wait_seconds}s...")
            time.sleep(wait_seconds)
            continue

        if response.status_code != 200:
            logger.error(f"Error: {response.status_code} — {response.text}")
            break

        repos = response.json()
        if not repos:
            break

        all_repos.extend(repos)
        params["page"] += 1

    return all_repos

def simplify_repo(repo: Dict) -> Dict:
    return {
        "name": repo.get("name"),
        "full_name": repo.get("full_name"),
        "stars": repo.get("stargazers_count", 0),
        "forks": repo.get("forks_count", 0),
        "license": repo["license"]["name"] if repo.get("license") else None,
        "language": repo.get("language"),
        "created_at": repo.get("created_at"),
        "updated_at": repo.get("updated_at"),
        "pushed_at": repo.get("pushed_at")
    }

def extract_data(save_raw: bool = True) -> List[Dict]:
    config = load_config()
    token = os.getenv("GITHUB_TOKEN") or config["github"].get("token")
    orgs = config["github"]["orgs"]
    headers = get_headers(token)

    all_data = []
    for org in orgs:
        repos = fetch_all_repos(org, headers)
        all_data.extend([simplify_repo(repo) for repo in repos])
        logger.info(f"{len(repos)} repos fetched from {org}")

    if save_raw:
        Path("data").mkdir(exist_ok=True)
        with open(DEFAULT_RAW_OUTPUT, "w", encoding="utf-8") as f:
            json.dump(all_data, f, indent=2)
        logger.info(f"Raw data saved to {DEFAULT_RAW_OUTPUT}")

    return all_data