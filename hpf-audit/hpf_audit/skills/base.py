"""
Skill 基类
所有业务 Skill 必须继承此类
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional


class BaseSkill(ABC):
    """Skill 基类"""
    
    def __init__(self, mcp_client=None):
        """
        初始化 Skill
        
        Args:
            mcp_client: MCP Client 实例（用于调用 MCP Servers）
        """
        self.mcp_client = mcp_client
    
    @property
    @abstractmethod
    def name(self) -> str:
        """技能名称（用于 Agent 调用）"""
        pass
    
    @property
    @abstractmethod
    def description(self) -> str:
        """
        技能描述（给 LLM 看的，帮助 Agent 决定何时调用此 Skill）
        应该清晰描述：
        1. 这个 Skill 的作用
        2. 适用场景
        3. 需要的参数
        """
        pass
    
    @property
    def input_schema(self) -> Dict[str, Any]:
        """
        输入参数 Schema（JSON Schema 格式）
        用于验证和文档生成
        """
        return {
            "type": "object",
            "properties": {},
            "required": []
        }
    
    @abstractmethod
    def execute(self, **kwargs) -> Dict[str, Any]:
        """
        执行技能
        
        Returns:
            字典格式的结果，必须包含：
            {
                "success": bool,
                "data": Any,
                "message": str (可选)
            }
        """
        pass
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典（用于 Agent 注册）"""
        return {
            "name": self.name,
            "description": self.description,
            "input_schema": self.input_schema
        }
