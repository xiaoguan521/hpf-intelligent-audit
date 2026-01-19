"""
Skills 模块初始化
注册所有可用的 Skills
"""
from typing import List
from .base import BaseSkill
from .mcp_client import MCPClient
from .withdrawal_audit import WithdrawalAuditSkill
from .loan_compliance import LoanComplianceSkill
from .internal_control import InternalControlSkill
from .data_analysis import DataAnalysisSkill
from .organization_audit import OrganizationAuditSkill
from .sql_executor import SQLExecutorSkill
from .risk_feedback import RiskFeedbackSkill


class SkillRegistry:
    """Skill 注册中心"""
    
    def __init__(self):
        self._skills: List[BaseSkill] = []
        self._mcp_client = MCPClient()  # 创建 MCP 客户端
        self._register_all_skills()
    
    def _register_all_skills(self):
        """注册所有 Skills（包括预定义的和数据库中的）"""
        # 1. 注册预定义的Skills
        self._skills = [
            WithdrawalAuditSkill(self._mcp_client),
            LoanComplianceSkill(self._mcp_client),
            InternalControlSkill(self._mcp_client),
            DataAnalysisSkill(self._mcp_client),
            OrganizationAuditSkill(self._mcp_client),  # 新增单位审计
            SQLExecutorSkill(),  # 新增 SQL 查询工具
            RiskFeedbackSkill(self._mcp_client), # 新增风险反馈
        ]
        
        # 2. 从数据库加载动态Skills（is_active=1）
        try:
            import sqlite3
            import yaml
            from hpf_audit.skills.template_engine import SkillFactory
            
            conn = sqlite3.connect('./housing_provident_fund.db')
            cursor = conn.cursor()
            cursor.execute("""
                SELECT skill_id, name, configuration 
                FROM META_SKILL_DEF 
                WHERE is_active = 1 
                AND configuration IS NOT NULL
            """)
            
            for row in cursor.fetchall():
                skill_id, name, config_yaml = row
                try:
                    # 解析YAML配置
                    config_dict = yaml.safe_load(config_yaml)
                    # 创建Skill实例
                    skill_instance = SkillFactory.create_skill(config_dict)
                    # 设置MCP客户端
                    skill_instance.mcp_client = self._mcp_client
                    self._skills.append(skill_instance)
                    print(f"✅ 加载数据库Skill: {skill_id} ({name})")
                except Exception as e:
                    print(f"⚠️  跳过Skill {skill_id}: {e}")
            
            conn.close()
        except Exception as e:
            print(f"⚠️  加载数据库Skills失败: {e}")
    
    def get_all_skills(self) -> List[BaseSkill]:
        """获取所有已注册的 Skills"""
        return self._skills
    
    def get_skill(self, name: str) -> BaseSkill:
        """根据名称获取 Skill"""
        for skill in self._skills:
            if skill.name == name:
                return skill
        return None
    
    def list_skills(self) -> list:
        """列出所有Skills的元数据"""
        return [
            {
                "name": skill.name,
                "description": skill.description,
                "input_schema": skill.input_schema
            }
            for skill in self._skills
        ]


# 全局单例
_skill_registry = None


def get_skill_registry() -> SkillRegistry:
    """获取全局 Skill Registry 单例"""
    global _skill_registry
    if _skill_registry is None:
        _skill_registry = SkillRegistry()
    return _skill_registry


__all__ = [
    "BaseSkill",
    "MCPClient",
    "WithdrawalAuditSkill",
    "LoanComplianceSkill",
    "InternalControlSkill",
    "DataAnalysisSkill",
    "OrganizationAuditSkill",
    "SQLExecutorSkill",
    "SkillRegistry",
    "get_skill_registry"
]

