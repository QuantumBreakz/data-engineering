# Deps to add into pyproject.toml depending on current MvP iteration

[project]
name = "github-data-lake"
version = "0.1.0"
description = "A medallion data lake pipeline consuming GitHub Events"
requires-python = ">=3.11"

# ========== MVP v1: Ingestion + Bronze/Silver (Parquet) ==========
dependencies = [
    # Kafka
    "confluent-kafka>=2.3.0",         # Mer stabil än kafka-python för produktion

    # GitHub API polling
    "requests>=2.31.0",
    "python-dotenv>=1.0.0",

    # Parquet + data manipulation
    "pyarrow>=15.0.0",                # Motorn bakom Parquet-läsning/skrivning
    "pandas>=2.2.0",                  # Bronze -> Silver transformationer i MVP v1

    # Validering (Det kan jag sedan glossary_db)
    "pydantic>=2.6.0",

    # Logging
    "loguru>=0.7.0",                  # Bättre än stdlib logging
]

# ========== MVP v2: PySpark + dbt ==========
# Lägg till när jag når MVP v2:
# "pyspark>=3.5.0" - DONE
# "dbt-core>=1.7.0"
# "dbt-spark>=1.7.0"

# ========== MVP v3: Airflow ==========
# Airflow kör i egen Docker-container med eget venv, inte här

[tool.uv]
dev-dependencies = [
    "pytest>=8.0.0",
    "pytest-cov>=4.1.0",
    "ruff>=0.3.0",
    "black>=24.0.0",
]

[tool.ruff]
line-length = 88
select = ["E", "F", "I"]             # Errors, Pyflakes, Isort

[tool.pytest.ini_options]
testpaths = ["tests"]