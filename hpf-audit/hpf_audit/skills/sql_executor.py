from typing import Dict, Any
from hpf_audit.skills.base import BaseSkill
from hpf_audit.skills.mcp_client import MCPClient

class SQLExecutorSkill(BaseSkill):
    """
    通用 SQL 执行 Skill
    允许 LLM 执行任意（安全的）SQL 查询以获取数据
    """
    
    def __init__(self):
        self._mcp_client = MCPClient()
        
    @property
    def name(self) -> str:
        return "safe_query"
        
    @property
    def description(self) -> str:
        return """
        通用 SQL 查询工具。用于执行 SELECT 查询以检索审计证据。
        支持 SQLite 语法。只读权限。
        
        输入参数:
        - sql: (string) SQL 查询语句
        - mask_data: (boolean, optional) 是否脱敏，默认 True
        """
        
    @property
    def input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "SELECT 查询语句"},
                "mask_data": {"type": "boolean", "default": True}
            },
            "required": ["sql"]
        }
        
    def execute(self, sql: str, mask_data: bool = True, **kwargs) -> Dict[str, Any]:
        return self._mcp_client.call("hpf-db-adapter", "safe_query", {
            "sql": sql, 
            "mask_data": mask_data
        })
