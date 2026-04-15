# run_pipeline script - CLI commands with ARGPARSE
# Code: English
import argparse
import sys
from loguru import logger

from config import LOG_LEVEL

# ========== Logging ==========
logger.remove()
logger.add(
    sink=lambda msg: print(msg, end=""),
    level=LOG_LEVEL,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | {message}",
)

# ========== Layer functions ==========
# Each function imports and runs its own transform script.
# Imports inside the functions (Lazy imports) instead of at top level.


# Run bronze to silver layer
def run_bronze() -> None:
    """Runs bronze-to-silver transformation."""
    logger.info("Pipeline: Starting Bronze -> Silver.")
    from transforms.bronze_to_silver import run_bronze_to_silver

    run_bronze_to_silver()
    logger.info("Pipeline: Bronze -> Silver complete!")


# Run silver -> gold layer
def run_silver() -> None:
    """Runs silver-to-gold transformation."""
    logger.info("Pipeline: Starting Silver -> Gold.")
    from transforms.silver_to_gold import run_silver_to_gold

    run_silver_to_gold()
    logger.info("Pipeline: Silver -> Gold complete!")


# Run EVERYTHING bronze -> gold layer
def run_all() -> None:
    """
    Runs the entire transformation chain in correct order.
    Bronze always needs to run BEFORE Silver.
    Silver always needs to run BEFORE Gold.

    This function ensures that human error is not part of the equation.
    """
    logger.info("Pipeline: Starting entire medallion run (Bronze -> Silver -> Gold)")
    run_bronze()
    run_silver()
    logger.info("Pipeline: Full run complete!")


# ========== CLI COMMANDS ==========
# Private function. Handle with care.
def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Github Data Lake - Pipeline Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run the entire pipeline (Bronze->Silver->Gold)
    uv run python -m scripts.run_pipeline --layer all

    # Run only Bronze -> Silver
    uv run python -m scripts.run_pipeline --layer bronze

    # Run only Silver -> Gold
    uv run python -m scripts.run_pipeline --layer silver
    """,
    )

    parser.add_argument(
        "--layer",
        type=str,
        choices=["bronze", "silver", "all"],
        required=True,
        help="Which layer to run: bronze, silver or all.",
    )

    return parser.parse_args()


# ========== Entrypoint ==========
if __name__ == "__main__":
    args = _parse_args()

    if args.layer == "bronze":
        run_bronze()
    elif args.layer == "silver":
        run_silver()
    elif args.layer == "all":
        run_all()
    else:
        logger.error(f"Unknown layer: {args.layer}")
        sys.exit(1)
# The final Else statement can never really happen because argparse validates choices before reaching it.
# But it is good practice to always have an explicit else statement that catches edge cases/unexpected falls.
