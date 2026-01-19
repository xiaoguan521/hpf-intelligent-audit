# hpf-common

住房公积金系统公共基础库

## 功能

纯工具库，不包含任何业务逻辑，提供：

- **LLM 客户端**: 统一接口支持 NVIDIA/OpenAI/Cerebras/Anthropic
- **DB 管理器**: 统一数据库连接 (SQLite/DuckDB/Oracle)
- **Embedding 客户端**: 向量化服务
- **配置管理**: Pydantic Settings
- **通用工具**: 日志、日期处理等

## 安装

```bash
# 开发安装
pip install -e .

# 安装可选依赖
pip install -e ".[llm,db,embedding]"
```

## 使用示例

```python
from hpf_common.llm import LLMClient
from hpf_common.db import DBManager

# LLM
client = LLMClient(provider="nvidia")
response = client.chat([{"role": "user", "content": "你好"}])

# 数据库
with DBManager.connect('sqlite', path='data.db') as conn:
    cursor = conn.cursor()
    ...
```

## 依赖项目

- hpf-audit (审计系统)
- hpf-platform (数据平台)
