FROM python:3.12-slim AS base

WORKDIR /app

RUN apt-get update && apt-get install -y libpq-dev gcc postgresql-client && rm -rf /var/lib/apt/lists/*

COPY ./backtesting_backend/ .
RUN pip install --no-cache-dir -r requirements.txt

# Development stage with hot reload
FROM base AS dev
CMD ["uvicorn", "main_query:app", "--host", "0.0.0.0", "--port", "8002", "--reload"]

# Production stage
FROM base AS prod
CMD ["uvicorn", "main_query:app", "--host", "0.0.0.0", "--port", "8002"]
