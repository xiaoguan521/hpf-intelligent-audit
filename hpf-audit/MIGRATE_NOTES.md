# hpf-audit 模块说明

## 当前状态

已从原项目复制核心代码到 `hpf_audit/`:
- `agent/` - ReAct Agent 引擎
- `skills/` - 审计 Skills
- `ml_scorer/` - ML 二次评分器（原 ml_engine）

## 下一步工作

需要修改 import 语句，使用 `hpf_common` 公共库：

### 需要修改的主要导入

1. **LLM 客户端** (暂无需修改，代码中已直接使用 llm_client 参数)
2. **数据库连接** 
   - 当前: `import sqlite3`
   - 修改为: `from hpf_common.db import DBManager`
   
3. **配置管理**
   - 未来: `from hpf_common.config import settings`

4. **Skills 内部导入**
   - 当前: `from skills import BaseSkill`
   - 修改为: `from hpf_audit.skills import BaseSkill`

5. **Agent/Skills 互相导入**
   - 当前: `from agent.react_engine import ReActAgent`
   - 修改为: `from hpf_audit.agent import ReActAgent`

## 暂时保持现状的原因

代码量较大，需要逐步重构。先确保项目结构正确，再批量修改导入。
