# 住房公积金智能审计系统 (重构版)

本项目采用模块化架构,由三个独立的 Python 包组成:

## 项目结构

```
test/
├── hpf-common/          # 公共基础库
├── hpf-audit/           # 审计系统
├── hpf-platform/        # 数据平台
├── .env                 # 环境配置
├── .venv/               # Python 虚拟环境
└── README.md            # 本文档
```

## 三大核心包

### 1. hpf-common - 公共基础库
提供统一的 LLM 客户端、数据库连接管理等基础功能。

**主要功能:**
- LLM 统一客户端 (支持 OpenAI、Anthropic、NVIDIA 等)
- 数据库连接管理 (SQLite、DuckDB、Oracle)
- 配置管理和环境变量

**安装:**
```bash
cd hpf-common
pip install -e ".[llm,db]"
```

### 2. hpf-audit - 审计系统
基于 ReAct Agent 的智能审计系统,包含 AI Skills、知识库和前端界面。

**主要功能:**
- ReAct Agent (智能对话和任务执行)
- AI Skills 生成和管理
- 知识库 (FAISS 向量检索)
- React 前端界面

**安装:**
```bash
cd hpf-audit
pip install -e .
```

**运行:**
```bash
# 启动后端
python run.py

# 启动前端 (另一个终端)
cd frontend-new
npm install
npm run dev
```

### 3. hpf-platform - 数据平台
Oracle → DuckDB 的 ETL 同步、dbt 数据建模和 ML 预测。

**主要功能:**
- ETL: Oracle → DuckDB 智能同步
- dbt: Bronze-Silver-Gold 数据分层
- ML: 逾期风险预测模型

**安装:**
```bash
cd hpf-platform
pip install -e .
```

**使用:**
```bash
# ETL 同步
python -m hpf_platform.etl.app --smart --auto

# dbt 建模
cd dbt_project
dbt run

# ML 训练
python -m hpf_platform.ml.train data/warehouse.duckdb
```

## 快速开始

### 1. 安装依赖

```bash
# 激活虚拟环境
source .venv/bin/activate

# 按顺序安装三个包
cd hpf-common && pip install -e ".[llm,db]"
cd ../hpf-audit && pip install -e .
cd ../hpf-platform && pip install -e .
```

### 2. 配置环境变量

复制 `.env.example` 为 `.env` 并配置:

```bash
cp .env.example .env
# 编辑 .env 文件,填入 API Keys 等配置
```

### 3. 运行系统

```bash
# 启动审计系统后端
cd hpf-audit
python run.py

# 启动前端 (新终端)
cd hpf-audit/frontend-new
npm run dev
```

访问 http://localhost:5173 查看前端界面。

## 数据流架构

```
Oracle 生产库
    ↓ (ETL)
DuckDB ODS 层
    ↓ (dbt Bronze)
原始数据视图
    ↓ (dbt Silver)  
清洗标准化
    ↓ (dbt Gold)
特征宽表
    ↓ (ML)
预测模型 → ReAct Agent → 前端界面
```

## Git 分支说明

- `1.0.0`: 重构前的完整代码 (保留备份)
- `1.0.1`: 重构后的模块化架构 (当前分支)

## 文档

- [hpf-common README](./hpf-common/README.md)
- [hpf-audit README](./hpf-audit/README.md)  
- [hpf-platform README](./hpf-platform/README.md)

## 技术栈

- **后端**: Python 3.9+, FastAPI, LangChain
- **前端**: React, TypeScript, Ant Design, Vite
- **数据**: DuckDB, FAISS, dbt
- **AI**: OpenAI, Anthropic, NVIDIA LLM

## License

内部项目
