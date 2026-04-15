# Bronze to silver script
# Code: English
import sys
import os
from pathlib import Path
from loguru import logger
import shutil
import json
from datetime import datetime, timezone

# PySpark imports
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

os.environ.setdefault("HADOOP_HOME", r"C:/Program Files/hadoop")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


from config import (
    BRONZE_DIR,
    SILVER_DIR,
    LOG_LEVEL,
    RELEVANT_EVENT_TYPES,
)

# ==== CHECKPOINT HANDLING ===
# The checkpoint file should live outside data folders so it is never accidentally cleared
# by shutil.rmtree when I clear silver partitions
CHECKPOINT_FILE = Path("data/checkpoints/bronze_to_silver.json")

# ========== LOGGING ==========
logger.remove()
logger.add(
    sink=lambda msg: print(msg, end=""),
    level=LOG_LEVEL,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | {message}",
)


# ========== DLQ PATH ==========
# Dead Letter Queue lives next to silver, not in silver. Unexpected data has its own place
# so that I can inspect it later without cluttering the silver layer
DLQ_DIR = Path("data/dlq/events")


# ========== CHECKPOINT FUNCTIONS ==========
def _load_checkpoint() -> set[str]:
    """
    Reads the checkpoint file and returns an array of already processed
    file paths. Returns an empty array if the file does not yet exist
    this is expected behavior on first run.

    Uses a set (array) instead of a list for an important
    reason: checking if a file has already been processed is O(1)
    with a set, versus O(n) with a list. When you have thousands
    of checkpointed files, this matters a lot.
    """
    if not CHECKPOINT_FILE.exists():
        logger.info("No checkpoint found - Will process ALL Bronze files.")
        return set()

    with open(CHECKPOINT_FILE, "r") as f:
        data = json.load(f)
        processed = set(data.get("processed_files", []))
        last_run = data.get("last_run", "unknown")
        logger.info(
            f"Checkpoint loaded | {len(processed)} files already processed | last_run={last_run}"
        )
        return processed


def _save_checkpoint(processed_files: set[str]) -> None:
    """
    Writes an updated checkpoint file to disk after a successful run.
    I ALWAYS write the checkpoint after the Silver data is safely on disk,
    never before. Same principle as offset-commit in consumer.py:
    data to disk is always priority one.
    """
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(
            {
                "processed_files": list(processed_files),
                "last_run": datetime.now(timezone.utc).isoformat(),
            },
            f,
            indent=2,
        )
    logger.info(f"Checkpoint saved | {len(processed_files)} total processed files")


# ========== Main Function ==========
def run_bronze_to_silver() -> None:
    """
    Reads Bronze-layer with PySpark, transforms to Silver-layer.

    Flow:
    Read Parquet -> Filter event-types -> Deduplicate ->
    Flatten via native Spark -> Clear silver partitions -> Write Silver
    """
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("github-data-lake-bronze-to-silver")
        # Tell Spark to write Parquet with Hive-style partitioning
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    # Set log level to ERROR so Spark doesn't spam the terminal with WARN
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("Starting Bronze -> Silver transformation (PySpark)")

    # ========== INCREMENTAL FILE SELECTION ==========
    # Find ALL Bronze files and filter out those I already processed.
    all_bronze_files = [str(p) for p in BRONZE_DIR.rglob("*.parquet")]
    processed_files = _load_checkpoint()

    new_files = [f for f in all_bronze_files if f not in processed_files]

    if not new_files:
        logger.info("No new Bronze files since last run - Nothing to do!")
        spark.stop()
        return

    logger.info(
        f"Found {len(all_bronze_files)} total Bronze files | "
        f"{len(processed_files)} already processed | "
        f"{len(new_files)} new files to process"
    )

    # Load ONLY the NEW files, NOT the entire Bronze layer.
    df_bronze = spark.read.parquet(*new_files)
    total = df_bronze.count()
    logger.info(f"Loaded {total} new events from Bronze layer.")

    # ========== Filter on known event types ==========
    df_filtered = df_bronze.filter(F.col("type").isin(list(RELEVANT_EVENT_TYPES)))
    logger.info(f"After event-type filter: {df_filtered.count()} events")

    # ========== Deduplication ==========
    df_deduped = df_filtered.dropDuplicates(["id"])
    dupes = df_filtered.count() - df_deduped.count()
    if dupes > 0:
        logger.info(f"Removed {dupes} duplicate events")

    # ========== Flattening with built-in Spark functions ==========
    # I skip the UDF entirely! get_json_object extracts data directly in the JVM engine.
    df_silver = df_deduped.select(
        F.col("id").cast("string").alias("event_id"),
        F.col("type").cast("string").alias("event_type"),
        # 1) actor and repo were saved as 'Structs' (nested objects) in Parquet.
        # Therefore, I can use dot notation directly! No need for json functions.
        F.col("actor.login").cast("string").alias("actor_login"),
        F.col("repo.name").cast("string").alias("repo_name"),
        F.col("repo.id").cast("string").alias("repo_id"),
        # 2) payload was saved as a raw text string in Bronze.
        # I therefore keep get_json_object here to dig in the str
        # Use coalesce to fill with 0 if "size" is missing
        F.coalesce(
            F.get_json_object(F.col("payload"), "$.size").cast("integer"), F.lit(0)
        ).alias("commit_count"),
        F.coalesce(
            F.get_json_object(F.col("payload"), "$.pull_request.number").cast(
                "integer"
            ),
            F.lit(0),
        ).alias("pr_number"),
        F.get_json_object(F.col("payload"), "$.action").alias("pr_action"),
        # Coalesce to give default False if "merged" is missing
        F.coalesce(
            F.get_json_object(F.col("payload"), "$.pull_request.merged").cast(
                "boolean"
            ),
            F.lit(False),
        ).alias("pr_merged"),
        F.col("created_at").cast("string"),
    )

    # Trigger cache to force calculation
    df_silver.cache()
    silver_count = df_silver.count()
    logger.info(f"Flattened {silver_count} events to Silver schema")

    # ========== Clear affected silver partitions ==========
    # ==========        (Idempotency)              ==========
    dates = df_silver.select(F.to_date("created_at").alias("date")).distinct().collect()

    for row in dates:
        dt = row["date"]
        partition = SILVER_DIR / f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
        if partition.exists():
            shutil.rmtree(partition)
            logger.info(f"Cleared Silver partition: {partition}")

    # ========== Write to silver ==========
    (
        df_silver.withColumn("year", F.year(F.to_timestamp("created_at")))
        # Date_format with "MM" and "dd" forces leading zeros, e.g.: month=03
        .withColumn("month", F.date_format(F.to_timestamp("created_at"), "MM"))
        .withColumn("day", F.date_format(F.to_timestamp("created_at"), "dd"))
        .write.mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(str(SILVER_DIR))
    )

    logger.info(f"Wrote {silver_count} Silver records -> {SILVER_DIR}")

    # ===== Save checkpoint AFTER Silver is safely on disk =====
    # If I saved the checkpoint BEFORE the write and then crashed
    # the system would be tricked into believing the files are processed but
    # Silver data would be missing. Data to disk always priority 1, 2 and 3.
    updated_processed = processed_files | set(new_files)
    _save_checkpoint(updated_processed)

    logger.info("Bronze -> Silver transformation complete")

    spark.stop()


if __name__ == "__main__":
    run_bronze_to_silver()
in__":
    run_bronze_to_silver()
