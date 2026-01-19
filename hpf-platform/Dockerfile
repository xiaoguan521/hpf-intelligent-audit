# ==========================================
# Stage 1: Base Image (系统依赖 - 变化少)
# ==========================================
FROM python:3.9-slim AS base

WORKDIR /app

# 安装系统依赖（这层很少变化，缓存稳定）
RUN apt-get update && apt-get install -y \
    build-essential \
    libaio1t64 \
    curl \
    git \
    sed \
    && rm -rf /var/lib/apt/lists/*

# ==========================================
# Stage 2: Dependencies (依赖安装 - 中等变化)
# ==========================================
FROM base AS dependencies

# 先复制 requirements，单独安装（这层变化较少）
COPY hpf-common/setup.py hpf-common/README.md /tmp/hpf-common/
COPY hpf-common/hpf_common /tmp/hpf-common/hpf_common
WORKDIR /tmp/hpf-common

# 安装 hpf-common 依赖
RUN pip install --no-cache-dir -e .[llm,db]

# 安装 ML 相关依赖（变化更少）
RUN pip install --no-cache-dir \
    scikit-learn \
    xgboost \
    catboost \
    lightgbm \
    dbt-core \
    dbt-duckdb \
    faker \
    joblib \
    tabulate

# ==========================================
# Stage 3: Application (应用代码 - 变化频繁)
# ==========================================
FROM dependencies AS final

WORKDIR /app

# 复制 hpf-common（使用已安装的依赖）
COPY hpf-common /app/hpf-common

# 复制并安装 hpf-platform
COPY hpf-platform /app/hpf-platform
WORKDIR /app/hpf-platform

RUN pip install -e . --no-cache-dir

# 环境变量设置
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# 默认命令
CMD ["tail", "-f", "/dev/null"]
