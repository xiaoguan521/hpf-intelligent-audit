# ==========================================
# hpf-audit-backend Dockerfile
# ==========================================
FROM python:3.9-slim

WORKDIR /app

# 1. Install System Dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 2. Copy and Install hpf-common (Local Dependency)
COPY hpf-common /app/hpf-common
RUN pip install -e /app/hpf-common[llm,db,embedding] --no-cache-dir

# 3. Copy and Install hpf-audit
COPY hpf-audit /app/hpf-audit
WORKDIR /app/hpf-audit
RUN pip install -e . --no-cache-dir

# 4. Environment Setup
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# 5. Run Application
CMD ["python", "run.py"]
