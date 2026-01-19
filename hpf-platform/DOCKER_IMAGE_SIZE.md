# Docker 镜像大小分析与优化

## 📊 当前镜像大小构成

### 预估大小：**~1.2-1.5 GB**

**分层分析**：

| 层级 | 组件 | 大小 | 占比 |
|------|------|------|------|
| **基础镜像** | python:3.9-slim | ~150 MB | 10% |
| **系统依赖** | build-essential, git, etc | ~200 MB | 13% |
| **ML 核心库** | | | |
| - XGBoost | | ~100 MB | 7% |
| - CatBoost | | ~150 MB | 10% |
| - LightGBM | | ~50 MB | 3% |
| - scikit-learn | | ~80 MB | 5% |
| **数据处理** | | | |
| - pandas | | ~120 MB | 8% |
| - numpy | | ~80 MB | 5% |
| **dbt 生态** | | | |
| - dbt-core | | ~100 MB | 7% |
| - dbt-duckdb | | ~50 MB | 3% |
| - DuckDB | | ~80 MB | 5% |
| **其他依赖** | faker, plotly, etc | ~300 MB | 20% |
| **应用代码** | hpf-common + hpf-platform | ~40 MB | 3% |

**总计**: ~1.5 GB

---

## 🎯 优化方案

### 方案 A: 使用 Alpine 基础镜像 ❌ 不推荐

```dockerfile
FROM python:3.9-alpine
```

**问题**：
- ML 库编译困难（缺少预编译包）
- 构建时间增加 3-5倍
- 兼容性问题多

**节省**: ~100 MB（不值得）

---

### 方案 B: 多阶段构建优化 ✅ 已实施

```dockerfile
# 构建阶段
FROM python:3.9-slim AS builder
RUN pip install ...

# 运行阶段（只复制必要文件）
FROM python:3.9-slim
COPY --from=builder /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages
```

**节省**: ~200-300 MB

---

### 方案 C: 按需安装（推荐）⭐

**训练镜像** vs **预测镜像** 分离：

```yaml
# docker-compose.ml-train.yml (训练专用，大但全)
image: ghcr.io/.../hpf-ml-trainer:latest  # 1.5 GB
includes: XGBoost, CatBoost, LightGBM, dbt

# docker-compose.ml-predict.yml (预测专用，小而快)
image: ghcr.io/.../hpf-ml-predictor:latest  # 500 MB
includes: 仅 scikit-learn (加载模型)
```

**优势**：
- 训练环境：完整但大（1.5 GB）
- 生产环境：精简快速（500 MB）

---

### 方案 D: 清理缓存和临时文件 ✅ 可立即实施

```dockerfile
# 在每个 RUN 命令后清理
RUN pip install --no-cache-dir ... \
    && rm -rf /root/.cache/pip \
    && find /usr/local/lib/python3.9 -type d -name __pycache__ -exec rm -rf {} + \
    && find /usr/local/lib/python3.9 -name "*.pyc" -delete

# 清理 apt 缓存
RUN apt-get update && apt-get install -y ... \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
```

**节省**: ~150-200 MB

---

### 方案 E: 选择性安装 ML 库

**按使用场景安装**：

```dockerfile
# 最小训练环境（只用 CatBoost）
RUN pip install catboost scikit-learn dbt-core dbt-duckdb
# 大小: ~800 MB

# 完整训练环境（测试所有算法）
RUN pip install xgboost catboost lightgbm scikit-learn dbt-core dbt-duckdb
# 大小: ~1.5 GB
```

---

## 🚀 立即可用的优化版 Dockerfile

```dockerfile
# ==========================================
# Stage 1: Base (系统依赖)
# ==========================================
FROM python:3.9-slim AS base

WORKDIR /app

# 合并安装和清理到一个 RUN 层
RUN apt-get update && apt-get install -y \
    build-essential \
    libaio1t64 \
    curl \
    git \
    sed \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /tmp/* /var/tmp/*

# ==========================================
# Stage 2: Dependencies (Python 依赖)
# ==========================================
FROM base AS dependencies

COPY hpf-common/pyproject.toml hpf-common/README.md /tmp/hpf-common/
COPY hpf-common/hpf_common /tmp/hpf-common/hpf_common
WORKDIR /tmp/hpf-common

RUN pip install --no-cache-dir -e .[llm,db] \
    && rm -rf /root/.cache/pip

# ML 依赖（优化版 - 只安装最常用的）
RUN pip install --no-cache-dir \
    scikit-learn \
    catboost \
    dbt-core \
    dbt-duckdb \
    faker \
    joblib \
    tabulate \
    && rm -rf /root/.cache/pip \
    && find /usr/local/lib/python3.9 -type d -name __pycache__ -exec rm -rf {} + \
    && find /usr/local/lib/python3.9 -name "*.pyc" -delete

# XGBoost 和 LightGBM 可选安装（需要时取消注释）
# RUN pip install --no-cache-dir xgboost lightgbm

# ==========================================
# Stage 3: Application (应用代码)
# ==========================================
FROM dependencies AS final

WORKDIR /app

COPY hpf-common /app/hpf-common
COPY hpf-platform /app/hpf-platform
WORKDIR /app/hpf-platform

RUN pip install -e . --no-cache-dir \
    && rm -rf /root/.cache/pip

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["tail", "-f", "/dev/null"]
```

**预期大小**: ~900-1000 MB（节省 30-40%）

---

## 📈 对比表

| 优化方案 | 镜像大小 | 构建时间 | 功能完整性 | 推荐度 |
|---------|---------|---------|-----------|--------|
| **当前版本** | 1.5 GB | 10 min | ✅✅✅ | - |
| **方案 B+D** | 1.0 GB | 10 min | ✅✅✅ | ⭐⭐⭐ |
| **方案 C (分离)** | 500 MB (prod) | 5 min | ✅✅ | ⭐⭐⭐⭐ |
| **方案 E (选择性)** | 800 MB | 8 min | ✅✅ | ⭐⭐⭐ |

---

## 💡 建议

### 短期（立即实施）
1. ✅ 添加清理命令（方案 D）
2. ✅ 只安装 CatBoost（方案 E）

**预期**: 1.5 GB → **1.0 GB**

### 中期（下次迭代）
1. ⭐ 创建 predictor 精简镜像
2. 保留 trainer 完整镜像

**预期**: 生产环境 **500 MB**

### 长期
1. 考虑使用预构建的 ML 基础镜像
2. 定期审查依赖，移除不用的库

---

## 🔍 查看当前镜像详情

```bash
# 查看镜像大小
docker images ghcr.io/xiaoguan521/hpf-intelligent-audit/hpf-ml-trainer

# 查看镜像各层大小
docker history ghcr.io/xiaoguan521/hpf-intelligent-audit/hpf-ml-trainer:latest
```

---

## ✅ 结论

**1.5 GB 对于完整 ML 训练环境是正常的**！

包含：
- 4+ 个 ML 框架
- 完整 dbt 生态
- 数据处理工具链

如需优化，推荐**方案 C（训练/预测分离）**，生产环境可减至 **500 MB**。
