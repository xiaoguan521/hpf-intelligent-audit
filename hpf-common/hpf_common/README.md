# Core - 公共基础库

这个目录包含项目的公共基础工具，不包含任何业务逻辑。

## 模块说明

- `llm/` - LLM 客户端统一接口
- `db/` - 数据库连接管理
- `embedding/` - 向量化客户端
- `config/` - 配置管理
- `utils/` - 通用工具函数

## 使用方式

```python
from core.llm import LLMClient
from core.db import DBManager
from core.config import settings

# 使用 LLM
llm = LLMClient()
response = llm.chat([{"role": "user", "content": "你好"}])

# 使用数据库
with DBManager.connect('sqlite', path='data.db') as conn:
    cursor = conn.cursor()
    ...
```

## 注意事项

- ⚠️ 该目录将来会被提取为独立的 `hpf-common` 包
- ✅ 只包含工具代码，不包含业务逻辑
- ✅ 其他模块通过 `from core.xxx import` 使用
