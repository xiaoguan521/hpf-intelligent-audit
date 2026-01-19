"""
通用 Skill 执行引擎 (Skill Runner)
负责解析 Markdown Skill 定义，并驱动 Agent 执行审计任务
"""
import sqlite3
import os
import json
from typing import Dict, Any, List, Optional

from hpf_audit.agent.react_engine import ReActAgent
from hpf_audit.skills.mcp_client import MCPClient
from hpf_audit.skills.sql_executor import SQLExecutorSkill

class SkillRunner:
    """Skill 执行器"""
    
    def __init__(self, db_path: str = "./housing_provident_fund.db"):
        self.db_path = db_path
        self.mcp_client = MCPClient(db_path)
        # 使用统一的 LLM 客户端
        from hpf_common.llm import LLMClient
        self.llm_client = LLMClient(verbose=True)
        self.sql_skill = SQLExecutorSkill()

    def _get_skill_definition(self, skill_id: str) -> Optional[Dict]:
        """从数据库读取 Markdown Skill 定义"""
        try:
            conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM META_SKILL_DEF WHERE skill_id = ?", (skill_id,))
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return dict(row)
            return None
        except Exception as e:
            print(f"Error loading skill: {e}")
            return None

    def _get_schema_context(self) -> str:
        """获取所有表的 Schema 上下文"""
        resources = self.mcp_client.list_resources("hpf-db-adapter")
        context_parts = []
        
        for res in resources:
            uri = res["uri"]
            schema = self.mcp_client.read_resource("hpf-db-adapter", uri)
            context_parts.append(schema)
            
        return "\n\n".join(context_parts)

    def run(self, skill_id: str) -> Dict[str, Any]:
        """运行指定的 Skill"""
        
        # 1. 加载 Skill 定义
        skill_def = self._get_skill_definition(skill_id)
        if not skill_def:
            return {"error": f"Skill ID {skill_id} not found"}
        
        print(f"Executing Skill: {skill_def['name']}")
        
        # 2. 准备 Schema 上下文
        schema_context = self._get_schema_context()
        
        # 3. 初始化 Agent
        agent = ReActAgent(
            llm_client=self.llm_client,
            skills=[self.sql_skill], # 仅提供 SQL 执行工具
            schema_context=schema_context,
            verbose=True
        )
        
        # 4. 构造任务指令
        # 将 Markdown 内容作为用户的任务指令
        task_instruction = f"""
请执行以下审计技能定义的任务：

{skill_def['markdown_content']}

请根据 Schema 上下文生成 SQL，分析数据，并输出符合 Skill 定义的结论。
"""
        
        # 5. 执行
        result = agent.run(task_instruction)
        
        return result

# 用于测试
if __name__ == "__main__":
    runner = SkillRunner()
    # 假设数据库中已有 'SKILL_001'
    res = runner.run("SKILL_001")
    print("\nResult:", json.dumps(res, indent=2, ensure_ascii=False))
