# Config.py
# Code: English
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

# Path(__file__) is this file. .parent is the folder config lies in (root)
# All data paths are built relative to root so the project works regardless of where it is on the machine.
ROOT_DIR = Path(__file__).parent
DATA_DIR = ROOT_DIR / "data"

BRONZE_DIR = DATA_DIR / "bronze" / "events"
SILVER_DIR = DATA_DIR / "silver" / "events"
GOLD_DIR = DATA_DIR / "gold"

# --- Kafka ---
# Producer and consumer import from here, independent of each other
# They communicate through the same topic since both read from the same config
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
KAFKA_TOPIC_RAW = "github-events-raw"  # Bronze. Raw data, untouched
KAFKA_TOPIC_DLQ = "github-events-dlq"  # Dead letter queue, invalid events
KAFKA_GROUP_ID = "github-lake-consumer-group"

# --- Github API ---
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")  # Automatically None if not set, 60 requests/h
GITHUB_EVENTS_URL = "https://api.github.com/events?per_page=100"
POLL_INTERVAL_SEC = 120  # 120 seconds between each poll, 2 min.

# -- Event types I care about ---
RELEVANT_EVENT_TYPES = {
    "PushEvent",
    "PullRequestEvent",
    "WatchEvent",
    "ForkEvent",
    "IssueCommentEvent",
    "CreateEvent",
}

# --- Repos/topics that indicate it might be DE community ---
DE_KEYWORDS = [
    "dbt",
    "airflow",
    "spark",
    "kafka",
    "flink",
    "dagster",
    "prefect",
    "duckdb",
    "delta-lake",
    "iceberg",
    "trino",
    "pyspark",
    "polars",
    "data-engineering",
    "data-engineer",
]

# --- Logging ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_DIR = ROOT_DIR / "logs"

# --- Partitioning ---
# Uses hive style folder structure in data/bronze year/month/day
# PySpark should understand this structure without having to change anything or think about anything specific.
DATE_PARTITION_FORMAT = "year={year}/month={month:02d}/day={day:02d}"
