# 住房公积金智能审计系统 (HPF Intelligent Audit)

![Build Status](https://github.com/xiaoguan521/hpf-intelligent-audit/actions/workflows/docker-publish.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.9%2B-blue)
![Docker](https://img.shields.io/badge/docker-ready-green)

**hpf-intelligent-audit** 是下一代住房公积金审计平台，采用基于 ReAct Agent 的智能审计架构与现代化的数据处理栈。

## 🏗️ 核心架构

本项目采用模块化 Monorepo 架构，包含三个核心子系统：

| 模块 | 目录 | 描述 | 技术栈 |
|------|------|------|--------|
| **hpf-audit** | `hpf-audit/` | **智能审计核心**。包含后端 API 和前端界面。基于 ReAct Agent，集成了 AI 技能 (Skills) 与知识库检索 (RAG)。 | FastAPI, React, LangChain |
| **hpf-platform** | `hpf-platform/` | **数据智能平台**。提供数据基础设施，负责 Oracle 到 DuckDB 的 ETL 同步、dbt 分层建模及 ML 风险预测。 | DuckDB, dbt, Scikit-learn |
| **hpf-common** | `hpf-common/` | **公共基础库**。统一的基础设施层，封装了多模态 LLM 客户端 (OpenAI/NVIDIA) 和异构数据库管理。 | Python |

## ✨ 核心特性

- **智能审计 Agent**: 基于 ReAct 框架，自主规划审计路径，调用工具查询数据。
- **现代化数据栈**: 使用 DuckDB 作为高性能 OLAP 引擎，dbt 管理 Bronze/Silver/Gold 数据分层。
- **机器学习集成**: 内置逾期风险预测模型，自动识别高风险贷款。
- **知识库 RAG**: 基于向量检索的政策法规问答。
- **云原生部署**: 支持 Docker 容器化部署，适配多架构 (AMD64/ARM64)。

## 🚀 快速开始 (Docker)

最简单的运行方式是使用 Docker Compose。

1. **克隆仓库**
   ```bash
   git clone git@github.com:xiaoguan521/hpf-intelligent-audit.git
   cd hpf-intelligent-audit
   ```

2. **配置环境变量**
   ```bash
   cp .env.example .env
   # 编辑 .env 填入 LLM_API_KEY 等信息
   ```

3. **启动服务**
   ```bash
   docker-compose up -d
   ```

服务启动后，可以通过浏览器访问：
- **前端界面**: http://localhost
- **后端 API**: http://localhost:8000/docs

## 🛠️ 本地开发指南

如果您需要进行代码开发，建议在虚拟环境中运行。

### 环境准备

1. **创建虚拟环境**
   ```bash
   python3 -9 -m venv .venv
   source .venv/bin/activate
   ```

2. **安装依赖 (按顺序)**
   ```bash
   # 1. 安装基础库
   pip install -e "./hpf-common[llm,db]"

   # 2. 安装数据平台
   pip install -e "./hpf-platform"

   # 3. 安装审计系统
   pip install -e "./hpf-audit"
   ```

### 运行模块

**运行 ETL 与 ML 任务:**
```bash
# 运行智能同步
python -m hpf_platform.etl.app --smart --auto

# 运行 dbt 模型
cd hpf-platform/dbt_project && dbt run

# 训练预测模型
python -m hpf_platform.ml.train data/warehouse.duckdb
```

**运行审计系统:**
```bash
# 启动后端
cd hpf-audit
python run.py

# 启动前端 (需 Node.js 环境)
cd frontend
npm install && npm run dev
```

## 📂 项目结构

```
.
├── hpf-audit/           # [应用层] 审计业务系统
│   ├── hpf_audit/       # Python 后端代码
│   ├── frontend/        # React 前端代码
│   └── backend.Dockerfile
├── hpf-platform/        # [数据层] 数据处理平台
│   ├── hpf_platform/    # ETL & ML 代码
│   ├── dbt_project/     # dbt 数据模型
│   └── Dockerfile
├── hpf-common/          # [基础层] 公共依赖库
├── docker-compose.yml   # 容器编排配置
└── .github/             # GitHub Actions工作流
```

## 📄 License

Internal Project. All rights reserved.
