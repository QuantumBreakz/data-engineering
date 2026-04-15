# Silver to gold script to aggregate silver data into gold layer (enrich data for analysis)
# Code: English
import shutil
from pathlib import Path
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import sys

os.environ.setdefault("HADOOP_HOME", r"C:/Program Files/hadoop")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from config import SILVER_DIR, LOG_LEVEL

# ========== LOGGING ==========
logger.remove()
logger.add(
    sink=lambda msg: print(msg, end=""),
    level=LOG_LEVEL,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | {message}",
)

# ===== Gold layer's three tables get their own folders under data/gold folders =====
GOLD_DIR = Path("data/gold")
TOOL_GROWTH = GOLD_DIR / "tool_growth"
ACTIVITY_MAP = GOLD_DIR / "activity_heatmap"
PR_CYCLES = GOLD_DIR / "pr_cycle_times"


# ===== Helper functions: Clear and write Gold partitions =====
# Private function. Handle with care.
def _write_gold(df, output_dir: Path, label: str) -> None:
    """
    Clears existing Gold table and writes a new version.
    Gold tables, unlike Silver, are not partitioned by day,
    they are aggregated across the entire dataset and written as a single table.
    Therefore, I clear the entire output_dir instead of individual day partitions.
    """
    if output_dir.exists():
        shutil.rmtree(output_dir)
        logger.info(f"Cleared Existing {label} table.")

    df.write.mode("overwrite").parquet(str(output_dir))
    logger.info(f"Wrote {label} -> {output_dir}")


# ========== GOLD 1: tool_growth ==========
# Track trends in the tools I look for from the DE community on Github
def build_tool_growth(df_silver) -> None:
    """
    Answers: which DE tools are growing the fastest in terms of stars per week?

    Filtering on WatchEvent (= a star on GitHub) and ForkEvent,
    group by repo and week, and count the number of events.
    date_trunc("week", ...) rounds a date down to Monday of the same week,
    this makes all days of the same week get the same week key,
    which is exactly what I want for a weekly aggregation.
    """
    logger.info("Building tool_growth..")

    df = (
        df_silver.filter(F.col("event_type").isin("WatchEvent", "ForkEvent"))
        # Converts created_at str to timestamp so that date_trunc works
        .withColumn("ts", F.to_timestamp("created_at"))
        .withColumn("week", F.date_trunc("week", F.col("ts")))
        .groupBy("repo_name", "week", "event_type")
        .agg(F.count("*").alias("event_count"))
        # Pivot: convert event_type rows to columns (stars, forks)
        # Before pivot:  repo | week | event_type | count
        # After pivot:  repo | week | stars      | forks
        .groupBy("repo_name", "week")
        .pivot("event_type", ["WatchEvent", "ForkEvent"])
        .agg(F.first("event_count"))
        .withColumnRenamed("WatchEvent", "stars")
        .withColumnRenamed("ForkEvent", "forks")
        # Fill null with 0, if a week had stars but no forks forks becomes null
        .fillna(0, subset=["stars", "forks"])
        .orderBy("repo_name", "week")
    )

    _write_gold(df, TOOL_GROWTH, "tool_growth")


# ========== GOLD 2: activity_heatmap ==========
def build_activity_heatmap(df_silver) -> None:
    """
    Answering: when is the DE community active?

    I extract the hour (0-23) and day of the week (1=Sunday, 7=Saturday in Spark)
    from created_at and count the total number of events per combination.
    The result is a 7x24 matrix, a heatmap, that shows which
    hours and days are most active. It is this table
    that hopefully produces the visually interesting pattern in Grafana later.
    """
    logger.info("Building activity_heatmap..")

    df = (
        df_silver.withColumn("ts", F.to_timestamp("created_at"))
        # hour() extracts the hour (0-23) from a timestamp
        .withColumn("hour_of_day", F.hour("ts"))
        # dayofweek() gives 1=Sunday, 2=Monday ... 7=Saturday
        .withColumn("day_of_week", F.dayofweek("ts"))
        # date_format gives me readable names: "Monday", "Tuesday" etc
        .withColumn("day_name", F.date_format("ts", "EEEE"))
        .groupBy("hour_of_day", "day_of_week", "day_name")
        .agg(F.count("*").alias("event_count"))
        .orderBy("day_of_week", "hour_of_day")
    )

    _write_gold(df, ACTIVITY_MAP, "activity_heatmap")


# ========== Gold 3: pr_cycle_times ==========
def build_pr_cycle_times(df_silver) -> None:
    """
    Answers: how long is a typical PR cycle in DE repos?

    This is the most complex aggregation, a self-join.
    Silver has separate rows for opened and closed PRs.
    I need to pair them together to calculate the time difference.
    """
    logger.info("Building pr_cycle_times..")

    # Filters only for PullRequestEvents
    df_pr = df_silver.filter(F.col("event_type") == "PullRequestEvent").withColumn(
        "ts", F.to_timestamp("created_at")
    )

    # Create TWO separate views of the same data, the core in self JOINs.
    # alias() gives each view a unique name so that Spark can distinguish them when joined.
    # Without alias Spark doesn't know which 'repo_name' I mean in the script.
    df_opened = (
        df_pr.filter(F.col("pr_action") == "opened")
        .select(
            F.col("repo_name"),
            F.col("pr_number"),
            F.col("ts").alias("opened_at"),
        )
        .alias("opened")
    )

    df_closed = (
        df_pr.filter((F.col("pr_action") == "closed") & F.col("pr_merged"))
        .select(
            F.col("repo_name"),
            F.col("pr_number"),
            F.col("ts").alias("closed_at"),
        )
        .alias("closed")
    )

    # Self JOIN. Pair opened with closed on the same repo. Add condition that closed must be AFTER opened
    # AND that the difference is "reasonable" < 30 days == 2,592,000 sec.
    df_joined = (
        df_opened.join(df_closed, on=["repo_name", "pr_number"], how="inner")
        .withColumn(
            "cycle_hours",
            (F.unix_timestamp("closed_at") - F.unix_timestamp("opened_at")) / 3600,
        )
        .filter(F.col("cycle_hours").between(0, 720))
    )

    # Aggregates per repo. Median and 95th percentile for cycle times.
    # percentile_approx == Spark's built-in function to calculate 'approximate percentile of a numeric column in a large dataset'
    # Exact percentile is not reasonable to calculate for large datasets..
    df_gold = (
        df_joined.groupBy("repo_name")
        .agg(
            F.count("*").alias("pr_count"),
            F.round(F.percentile_approx("cycle_hours", 0.5), 1).alias("median_hours"),
            F.round(F.percentile_approx("cycle_hours", 0.95), 1).alias("p95_hours"),
        )
        # Filter out repos with very few PRs that don't provide meaningful statistics
        .filter(F.col("pr_count") >= 5)
        .orderBy(F.col("median_hours").asc())
    )

    _write_gold(df_gold, PR_CYCLES, "pr_cycle_times")


# ========== MAIN FUNCTION ==========
# Read silver once and reuse for all three aggregations.
# cache() keeps my DF in memory so I don't need to read from disk 3x and don't need to worry about OOM issues!
def run_silver_to_gold() -> None:
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("github-data-lake-silver-to-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    logger.info("Starting Silver to Gold transformation with PySpark")

    df_silver = spark.read.parquet(str(SILVER_DIR)).cache()
    total = df_silver.count()
    logger.info(f"Loaded: {total} Silver records!")

    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    build_tool_growth(df_silver)
    build_activity_heatmap(df_silver)
    build_pr_cycle_times(df_silver)

    logger.info("Silver to Gold transformation is complete.")
    spark.stop()


if __name__ == "__main__":
    run_silver_to_gold()
"__main__":
    run_silver_to_gold()
