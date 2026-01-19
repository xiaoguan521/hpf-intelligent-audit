# hpf-platform

住房公积金数据平台

## 功能

数据平台，负责 Oracle → DuckDB 同步、数据建模、ML 预测：

- **ETL 引擎**: Oracle → DuckDB 智能同步
- **dbt 建模**: Bronze-Silver-Gold 分层，构建特征宽表
- **ML 预测**: 基于宽表的独立预测模型
- **API 服务**: FastAPI

## 数据流

```
Oracle 生产库
  ↓ ETL 同步
DuckDB ODS 层
  ↓ dbt 建模
DuckDB DW 层 (宽表)
  ↓ ML 训练
预测模型
```

## 安装

```bash
# 先安装依赖
cd ../hpf-common && pip install -e ".[llm,db]"

# 安装数据平台
cd ../hpf-platform
pip install -e .
```

## 使用

```bash
# ETL 同步
hpf-etl sync --smart --auto

# dbt 构建
cd dbt_project
dbt run

# 预测服务
hpf-platform serve --port 8001
```

## 目录结构

```
hpf_platform/
├── etl/         # ETL引擎
├── ml/          # ML预测
dbt_project/     # dbt独立项目
└── api/         # API服务
```
