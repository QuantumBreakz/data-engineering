# CLI argparse commands and how to run bootstrap_historical.py script

To run `bootstrap_historical.py` script using CLI commands they are this:
- `uv run python -m scripts.bootstrap_historical --days 1` one day
- `uv run python -m scripts.bootstrap_historical --days 7` one week
- `uv run python -m scripts.bootstrap_historical --days 30` one month

- `uv run python -m scripts.bootstrap_historical` one week as default
- `uv run python -m scripts.bootstrap_historical --start 2024-01-01 --end 2024-01-08` between certain intervals