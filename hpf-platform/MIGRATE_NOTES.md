# hpf-platform 模块说明

## 当前状态

已从原项目复制核心代码到 `hpf_platform/`:
- `etl/` - Oracle → DuckDB ETL 引擎
  - `app.py` - 主同步程序
  - `smart_sync.py` - 智能同步策略
  - `oracle_inspector.py` - Oracle 元数据分析
  - `sync_verifier.py` - 数据校验器
  - `config.py` - 配置管理

## 下一步工作

### 1. 修改 import 语句

需要将 ETL 代码中的导入改为使用 `hpf_common`:

- **LLM 客户端** (smart_sync.py 中)
  - 当前: 自己实现或从utils导入
  - 修改为: `from hpf_common.llm import LLMClient`
  
- **配置管理**
  - 当前: `from etl.config import OracleConfig`
  - 修改为: `from hpf_platform.etl.config import OracleConfig`
  - 同时使用: `from hpf_common.config import settings`

### 2. 建立 dbt 项目

在 `dbt_project/` 目录创建基础结构:
- `dbt_project.yml` - dbt 项目配置
- `profiles.yml` - 连接配置
- `models/bronze/` - ODS 层
- `models/silver/` - 清洗层
- `models/gold/` - 宽表层

### 3. 创建 ML Pipeline

在 `ml/` 目录创建:
- `features.py` - 从 DuckDB 加载特征
- `train.py` - 模型训练
- `serve.py` - 模型服务

## 暂时保持现状的原因

ETL 代码较复杂，需要确保功能正常后再逐步重构。
