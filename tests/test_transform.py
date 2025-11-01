from src.transform import transform_data
import pytest
import polars as pl
from src.transform import transform_data

# ---------- TEST 1: Structure and basic columns ----------
def test_transform_adds_expected_columns():
    raw_data = [{
        "name": "repo1",
        "full_name": "org/repo1",
        "stars": 10,
        "forks": 2,
        "license": {"name": "MIT"},
        "language": "Python",
        "created_at": "2025-01-01T00:00:00Z",
        "updated_at": "2025-10-01T00:00:00Z",
        "pushed_at": "2025-10-30T00:00:00Z",
    }]

    df = transform_data(raw_data)

    expected_cols = {
        "name",
        "full_name",
        "stars",
        "forks",
        "license",
        "language",
        "created_at",
        "updated_at",
        "pushed_at",
        "days_since_last_push",
        "star_fork_ratio",
    }

    for col in expected_cols:
        assert col in df.columns


# ---------- TEST 2: Basic transformation logic ----------
def test_transform_data_basic():
    """Test that transform_data adds expected columns and filters inactive repos."""
    
    # 2 repos, one active, one inactive
    raw_data = [
        {
            "name": "repo1",
            "full_name": "org/repo1",
            "stars": 10,
            "forks": 2,
            "license": {"name": "MIT"},
            "language": "Python",
            "created_at": "2025-01-01T00:00:00Z",
            "updated_at": "2025-01-02T00:00:00Z",
            "pushed_at": "2025-10-31T00:00:00Z"  # recent → active
        },
        {
            "name": "repo2",
            "full_name": "org/repo2",
            "stars": 5,
            "forks": 0,
            "license": {"name": "Apache"},
            "language": "C++",
            "created_at": "2020-01-01T00:00:00Z",
            "updated_at": "2020-01-01T00:00:00Z",
            "pushed_at": "2020-01-01T00:00:00Z"  # very old → inactive
        }
    ]

    # Run transformation
    df = transform_data(raw_data)

    # sanity checks
    assert isinstance(df, pl.DataFrame)
    assert "days_since_last_push" in df.columns
    assert "star_fork_ratio" in df.columns

    # Active repo should remain
    names = df["name"].to_list()
    assert "repo1" in names
    assert "repo2" not in names


# ---------- TEST 3: Null handling ----------
def test_null_license_and_language_filled():
    raw_data = [{
        "name": "repo2",
        "full_name": "org/repo2",
        "stars": 5,
        "forks": 0,
        "license": None,
        "language": None,
        "created_at": "2025-01-01T00:00:00Z",
        "updated_at": "2025-10-01T00:00:00Z",
        "pushed_at": "2025-10-30T00:00:00Z",
    }]

    df = transform_data(raw_data)
    assert df["license"][0] == "unknown"
    assert df["language"][0] == "unknown"



# ---------- TEST 4: Star/Fork ratio ----------
def test_star_fork_ratio():
    """Check star/fork ratio calculation is correct."""
    raw_data = [
        {
            "name": "repo3",
            "full_name": "org/repo3",
            "stars": 9,
            "forks": 3,
            "license": None,
            "language": "Python",
            "created_at": "2025-10-01T00:00:00Z",
            "updated_at": "2025-10-01T00:00:00Z",
            "pushed_at": "2025-10-29T00:00:00Z"
        }
    ]

    df = transform_data(raw_data)
    ratio = df["star_fork_ratio"][0]
    assert abs(ratio - 3.0) < 1e-6  # 9 stars / 3 forks = 3.0


# ---------- TEST 5: Invalid date handling ----------
def test_invalid_dates_are_flagged():
    raw_data = [{
        "name": "bad_date_repo",
        "full_name": "org/bad",
        "stars": 1,
        "forks": 1,
        "license": {"name": "MIT"},
        "language": "Python",
        "created_at": "invalid_date",
        "updated_at": "2025-10-01T00:00:00Z",
        "pushed_at": "2025-10-01T00:00:00Z",
    }]

    df = transform_data(raw_data)
    assert "created_at_is_invalid" in df.columns
    invalid_count = df.filter(pl.col("created_at_is_invalid")).height
    assert invalid_count == 1