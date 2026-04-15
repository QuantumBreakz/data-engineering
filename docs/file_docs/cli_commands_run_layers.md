# CLI commands to run Bronze -> Gold layer.


- To run bronze to silver layer:
    - `uv run python -m scripts.run_pipeline --layer bronze`

- To run silver to gold layer:
    - `uv run python -m scripts.run_pipeline --layer silver`

- To run Bronze -> Silver -> Gold layer:
    - `uv run python -m scripts.run_pipeline --layer all`