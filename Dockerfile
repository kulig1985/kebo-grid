FROM python:3.12-slim AS base

WORKDIR /app

# Rendszer függőségek
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev curl \
    && rm -rf /var/lib/apt/lists/*

# Python függőségek
COPY pyproject.toml .
RUN pip install --no-cache-dir -e .

# Forrás
COPY src/ ./src/
COPY alembic/ ./alembic/
COPY alembic.ini .

# Üres config.yaml fájl létrehozása – kötelező, hogy Docker fájlként mountolhassa!
RUN touch /app/config.yaml

ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1

EXPOSE 8080

# --- Bot engine target ---
FROM base AS bot
CMD ["python", "src/app/main.py", "config.yaml"]

# --- API target ---
FROM base AS api
CMD ["python", "src/app/api_main.py", "config.yaml"]
