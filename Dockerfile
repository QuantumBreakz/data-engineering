# Dockerfile
# Tunn Python-image - Jag behöver inget fancy, bara rätt deps installerade
FROM python:3.12-slim

WORKDIR /app

# Kopiera dependency-filen först (Docker cache-optimering)
# Om pyproject.toml inte ändrats behöver Docker inte installera om deps vid rebuild
COPY pyproject.toml .
COPY uv.lock .

# Installera uv och sedan projektets deps
RUN pip install uv && uv sync --no-dev

# Kopiera resten av koden
COPY . .