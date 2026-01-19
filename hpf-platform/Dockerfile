# ==========================================
# hpf-platform Dockerfile
# ==========================================
FROM python:3.9-slim

WORKDIR /app

# 1. Install System Dependencies (Oracle Client Requirements)
RUN apt-get update && apt-get install -y \
    build-essential \
    libaio1 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 2. Copy and Install hpf-common
COPY hpf-common /app/hpf-common
RUN pip install -e /app/hpf-common[llm,db] --no-cache-dir

# 3. Copy and Install hpf-platform
COPY hpf-platform /app/hpf-platform
WORKDIR /app/hpf-platform
RUN pip install -e . --no-cache-dir

# 4. Environment Setup
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# 5. Default Command (ETL Help)
# Using a loop to keep container alive for exec/cron if simpler
CMD ["tail", "-f", "/dev/null"]
