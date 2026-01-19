# dbt 数据建模项目

数据分层建模,实现 Bronze-Silver-Gold 架构。

## 项目结构

```
dbt_project/
├── dbt_project.yml       # 项目配置
├── profiles.yml          # 连接配置
├── models/
│   ├── bronze/          # ODS 层 (原始数据视图)
│   ├── silver/          # 清洗层 (标准化表)
│   └── gold/            # 宽表层 (特征工程)
├── macros/              # 自定义宏
└── tests/               # 数据质量测试
```

## 数据流

```
Oracle → ETL → DuckDB (oracle_data schema)
                ↓ dbt
         Bronze 层 (视图)
                ↓ dbt
         Silver 层 (表)
                ↓ dbt
         Gold 层 (宽表) → ML Pipeline
```

## 使用方法

```bash
cd dbt_project

# 测试连接
dbt debug

# 运行全部模型
dbt run

# 只运行特定层
dbt run --select bronze
dbt run --select silver
dbt run --select gold

# 运行测试
dbt test
```

## 层级说明

### Bronze 层 (ODS)
- **物化方式**: View
- **作用**: 直接映射 ETL 同步的原始数据
- **示例**: `src_contracts` (映射 `GR_DK_HT`)

### Silver 层 (清洗)
- **物化方式**: Table
- **作用**: 数据清洗、类型转换、标准化
- **示例**: `stg_contracts` (清洗后的合同数据)

### Gold 层 (宽表)
- **物化方式**: Table
- **作用**: 特征工程,为 ML 准备宽表
- **示例**: `fct_loan_features` (贷款特征宽表)

## 依赖

```bash
pip install dbt-duckdb
```
