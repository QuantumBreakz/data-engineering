# kafka_producer.py - For ingestion
# Code: English
import json
import time

import requests
from confluent_kafka import Producer
from loguru import logger

# Import my keywords and constants
from config import (
    DE_KEYWORDS,
    GITHUB_EVENTS_URL,
    GITHUB_TOKEN,
    KAFKA_BROKER,
    KAFKA_TOPIC_RAW,
    LOG_LEVEL,
    POLL_INTERVAL_SEC,
    RELEVANT_EVENT_TYPES,
)

# --- Logging ---
logger.remove()  # Removes loguru's default handler
logger.add(
    sink=lambda msg: print(msg, end=""),
    level=LOG_LEVEL,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | {message}",
)


# --- Kafka delivery callback ---
# confluent-kafka is async, queues msgs and sends in the background.
# This function is called automatically when Kafka confirms or denies a msg. Without it, I never know if something is dropped.
# Private function. DO NOT TOUCH
def _on_delivery(err, msg) -> None:
    if err:
        logger.error(f"Kafka delivery failed. Reason: {err}")
    else:
        logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}]")


# --- Github API ---
# Private function. DO NOT TOUCH
def _build_headers() -> dict:
    """Builds request header. Token is optional but gives 5k requests per hour instead of 60."""
    headers = {"Accept": "application/vnd.github.v3+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return headers


# --- Github API ---
# Private function. DO NOT TOUCH
def _is_de_relevant(event: dict) -> bool:
    """
    Check if an event is relevant to the DE community.

    Two criteria must both be true:
    1. The event type is in RELEVANT_EVENT_TYPES (PushEvent, PREvent etc.)
    2. The repo name contains a DE keyword (dbt, airflow, spark etc.)

    Why filter already in producer and not in silver?
    The GitHub Events API returns ~300 events per poll. The majority are
    irrelevant (people pushing to their private hello world repos).
    I filter early to avoid cluttering Bronze with data that I
    will never use. Bronze = raw data that I choose to save, not
    everything that exists.
    """
    event_type = event.get("type", "")
    repo_name = event.get("repo", {}).get("name", "").lower()

    if event_type not in RELEVANT_EVENT_TYPES:
        return False

    return any(keyword in repo_name for keyword in DE_KEYWORDS)


# --- Github API ---
def fetch_events(headers: dict) -> list[dict]:
    """
    Retrieves public events from Github API.
    Returns empty list on error so the poll loop never crashes.
    """
    try:
        response = requests.get(GITHUB_EVENTS_URL, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Github API request failed. Reason: {e}")
        return []


# --- Producer ---
def run_producer() -> None:
    """
    Main loop. Polls Github every POLL_INTERVAL_SEC seconds.
    Flow per poll cycle:
    fetch_events() -> filter -> produce() to Kafka -> sleep.
    """
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})
    headers = _build_headers()

    auth_status = (
        "authenticated (5000 req/h)" if GITHUB_TOKEN else "unauthenticated (60 req/h)"
    )
    logger.info(f"Producer started | Broker: {KAFKA_BROKER} | GitHub: {auth_status}")

    seen_ids: set[str] = set()  # Deduplication within the same session

    while True:
        events = fetch_events(headers)
        logger.info(f"Fetched {len(events)} events from Github API")

        sent = 0
        skipped_type = 0
        skipped_dupe = 0

        for event in events:
            event_id = event.get("id")

            # Skip duplicates. Github returns the same 300 events until new ones appear.
            # Without this if statement, I send the same event multiple times.
            if event_id in seen_ids:
                skipped_dupe += 1
                continue

            if not _is_de_relevant(event):
                skipped_type += 1
                continue

            seen_ids.add(event_id)

            # produce() is non-blocking, it puts msg in an internal queue
            # poll(0) gives Kafka a chance to send what has been queued.
            producer.produce(
                topic=KAFKA_TOPIC_RAW,
                key=event_id,
                value=json.dumps(event),
                callback=_on_delivery,
            )
            producer.poll(0)
            sent += 1

        # flush() blocks until all queued msgs are delivered.
        # Do it after each poll cycle, not after each event!!
        producer.flush()

        logger.info(
            f"Cycle complete | sent={sent} skipped_type={skipped_type}, skipped_dupe={skipped_dupe}"
        )

        # Keep seen ids manageable, clear if it grows too large!
        # 10k IDs = ~33 poll cycles without restart.
        if len(seen_ids) > 10_000:
            seen_ids.clear()
            logger.debug("seen_ids cache cleared!")

        logger.info(f"Sleeping {POLL_INTERVAL_SEC}s until next poll")
        time.sleep(POLL_INTERVAL_SEC)


# --- Entrypoint ---
if __name__ == "__main__":
    run_producer()
