# ==========================================
# hpf-platform Dockerfile (ML-Enhanced)
# ==========================================
FROM python:3.9-slim

WORKDIR /app

# 1. Install System Dependencies (Oracle Client + ML Requirements)
RUN apt-get update && apt-get install -y \
    build-essential \
    libaio1t64 \
    curl \
    git \
    sed \
    && rm -rf /var/lib/apt/lists/*

# 2. Copy and Install hpf-common
COPY hpf-common /app/hpf-common
RUN pip install -e /app/hpf-common[llm,db] --no-cache-dir

# 3. Copy and Install hpf-platform
COPY hpf-platform /app/hpf-platform
WORKDIR /app/hpf-platform

# 4. Install ML Dependencies
RUN pip install --no-cache-dir \
    scikit-learn \
    xgboost \
    catboost \
    lightgbm \
    dbt-core \
    dbt-duckdb \
    faker \
    joblib

RUN pip install -e . --no-cache-dir

# 5. Environment Setup
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# 6. Default Command
CMD ["tail", "-f", "/dev/null"]
