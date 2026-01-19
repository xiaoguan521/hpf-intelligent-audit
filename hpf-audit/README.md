# hpf-audit

住房公积金智能审计系统

## 功能

规则审计系统，对已发生业务进行实时分析和风险识别：

- **AI Agent**: ReAct 推理引擎
- **5大审计 Skills**: 提取/贷款/内控/单位/数据分析
- **ML Scorer**: 对 Skill 结果进行二次评分
- **知识库**: ChromaDB 向量检索
- **API 服务**: FastAPI + 前端页面

## 数据源

- SQLite (`data/housing_provident_fund.db`) - 6MB样本数据

## 项目结构

```
hpf-audit/
├── hpf_audit/          # 核心Python包
│   ├── agent/          # ReAct Agent引擎
│   ├── skills/         # 审计Skills
│   ├── ml_scorer/      # ML二次评分
│   ├── knowledge/      # 知识库管理
│   └── api/            # FastAPI应用 ✨
│       ├── main.py     # API入口
│       └── routes/     # API路由
├── frontend/           # 前端页面
├── data/               # SQLite数据库
├── run.py              # 启动脚本 ✨
└── pyproject.toml      # 项目配置
```

## 安装

```bash
# 1. 先安装依赖包
cd ../hpf-common
pip install -e ".[llm,db,embedding]"

# 2. 安装审计系统
cd ../hpf-audit
pip install -e .
```

## 使用

### 方式1: 命令行启动

```bash
python run.py
```

### 方式2: 模块启动

```bash
python -m uvicorn hpf_audit.api.main:app --reload --port 8000
```

### 访问

- API文档: http://localhost:8000/docs
- 前端页面: 将frontend目录配置到静态服务器
