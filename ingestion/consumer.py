# kafka consumer.py - For ingestion
# Code: English
import json
import time
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError, KafkaException
from loguru import logger

# Import from my config.py
from config import (
    BRONZE_DIR,
    DATE_PARTITION_FORMAT,
    KAFKA_BROKER,
    KAFKA_GROUP_ID,
    KAFKA_TOPIC_RAW,
    LOG_LEVEL,
)

# --- Logging ---
logger.remove()
logger.add(
    sink=lambda msg: print(msg, end=""),
    level=LOG_LEVEL,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | {message}",
)


# --- Constants ---
# Batch size controls how often I write to disk. 100x events OR 60 sec. Whichever comes first triggers writing.
# Too small batch = too many small Parquet files (inefficient for PySpark later).
# Too large batch = too long between writes (more data to lose during crash).
BATCH_SIZE = 100
BATCH_TIMEOUT_SEC = 60


# --- Parquet writing ---
def _write_batch_to_bronze(batch: list[dict]) -> None:
    """
    Writes a batch of events to Bronze layer as Parquet.

    Each event gets its own row. The path is built from the event's created_at
    so that the data ends up in the correct year/month/day partition automatically.

    Grouping the batch by date before writing, a batch can contain
    events from midnight onwards, e.g. 23:59 and 00:01, which go to
    different day folders.
    """
    # Group events per partition date
    partitions: dict[str, list[dict]] = {}

    for event in batch:
        # Github's created_at format "Y/M/D time Z"
        created_at = event.get("created_at", "")
        try:
            dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            # If timestamp is missing or invalid, use UTC now
            dt = datetime.now(timezone.utc)
            logger.warning(
                f"Invalid created_at for event {event.get('id')} using current UTC time"
            )

        # --- Serialize payload to JSON string ---
        # PyArrow infers schema from the batch when running from_pylist().
        # Payload varies structurally per event type, a PushEvent has
        # "commits" which is a list of objects, a WatchEvent doesn't have it at all.
        # which makes "commits" quietly disappear from the Bronze file.
        # Solution: serialize payload to a JSON string BEFORE PyArrow even
        # sees it. A string always has a consistent schema - it is just text.
        # In _flatten() I deserialize the string back using json.loads().
        event_copy = event.copy()
        if "payload" in event_copy:
            event_copy["payload"] = json.dumps(event_copy["payload"])

        partition_key = DATE_PARTITION_FORMAT.format(
            year=dt.year,
            month=dt.month,
            day=dt.day,
        )
        partitions.setdefault(partition_key, []).append(event_copy)

    # Write each partition to its own folder
    for partition_key, events in partitions.items():
        output_path = BRONZE_DIR / partition_key
        output_path.mkdir(parents=True, exist_ok=True)

        # Convert the list of dicts to a PyArrow table.
        # PyArrow arranges schema automatically from the data, I don't need to
        # define columns manually in Bronze. Raw data is saved as is.
        table = pa.Table.from_pylist(events)

        # Build a unique filename based on timestamp so that I never overwrite.
        # If consumer reruns (after crash) it creates a new file next to the old one.
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        output_file = output_path / f"part-{timestamp}.parquet"

        pq.write_table(table, output_file, compression="snappy")
        logger.info(f"Wrote {len(events)} events -> {output_file}")


# --- Consumer ---
def run_consumer() -> None:
    """
    Main loop. Consumes events from Kafka and writes them to Bronze.

    Flow:
    poll() from Kafka -> collect in batch -> batch full/timeout -> write Parquet -> commit offset

    Note the order: write to disk FIRST, commit to Kafka THEN.
    If script crash after writing but before committing, I rerun and write
    the same data again, that's harmless. If its committed first and then crashed
    I would lose the data permanently.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": KAFKA_GROUP_ID,
            # "earliest" means: if there's no saved offset for this
            # consumer group, start from the oldest message in the topic.
            # "latest" would mean: start from the latest and miss all historical.
            "auto.offset.reset": "earliest",
            # Handle commits manually (after disk write)
            # auto.commit=true would have committed on time, regardless of whether it wrote to disk.
            "enable.auto.commit": False,
        }
    )

    consumer.subscribe([KAFKA_TOPIC_RAW])
    logger.info(f"Consumer started | Broker: {KAFKA_BROKER} | Topic: {KAFKA_TOPIC_RAW}")

    batch: list[dict] = []
    last_flush = datetime.now(timezone.utc)

    try:
        while True:
            # poll(1.0) = wait max 1 second for a new message.
            # Returns None if timeout is reached without message.
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # No data came in, check if timeout-flush is needed
                pass
            elif msg.error():
                # PARTITION_EOF is not a real error, it just means
                # that I've reached the end of a partition right now. New messages
                # can still come in.
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(
                        f"End of partition reached: {msg.topic()} [{msg.partition()}]"
                    )
                else:
                    if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                        # Topic doesn't exist yet, producer hasn't sent anything
                        # Wait a bit and try again instead of crashing.
                        logger.warning("Topic not available yet, retrying in 5 sec..")
                        time.sleep(5)
                    else:
                        raise KafkaException(msg.error())
            else:
                # Deserialize JSON string back to a dict
                try:
                    event = json.loads(msg.value().decode("utf-8"))
                    batch.append(event)
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.error(f"Failed to deserialize message: {e}")

            # --- Flush logic: write batch if full OR timeout reached ---
            now = datetime.now(timezone.utc)
            elapsed = (now - last_flush).total_seconds()
            batch_full = len(batch) >= BATCH_SIZE
            timeout_reached = elapsed >= BATCH_TIMEOUT_SEC and len(batch) > 0

            if batch_full or timeout_reached:
                reason = "batch_full" if batch_full else "timeout"
                logger.info(f"Flushing batch | reason={reason} | size={len(batch)}")

                # STEP 1: Write to disk (critical step!!)
                _write_batch_to_bronze(batch)

                # STEP 2: Commit offset to Kafka (once data is safe)
                consumer.commit(asynchronous=False)

                batch.clear()
                last_flush = now

    except KeyboardInterrupt:
        # Ctrl+C, write what remains in the batch before it closes
        logger.info("Shutdown signal received")
        if batch:
            logger.info(f"Flushing remaining {len(batch)} events before exit")
            _write_batch_to_bronze(batch)
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()
        logger.info("Consumer closed cleanly")


# --- Entrypoint ---
if __name__ == "__main__":
    run_consumer()
