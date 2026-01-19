import json
from typing import Dict, Any, List
from jinja2 import Template
from hpf_audit.skills.base import BaseSkill
from hpf_audit.skills.models import SkillConfiguration
from hpf_audit.skills.mcp_client import MCPClient

class BaseTemplateSkill(BaseSkill):
    """
    Base class for all configuration-driven skills.
    Wraps a SkillConfiguration object.
    """
    def __init__(self, config: SkillConfiguration, db_path: str = "./housing_provident_fund.db"):
        self.config = config
        self.mcp_client = MCPClient(db_path)

    @property
    def name(self) -> str:
        return self.config.skill_id

    @property
    def description(self) -> str:
        desc = self.config.meta.description
        if self.config.parameters:
            desc += "\nParameters:\n"
            for param in self.config.parameters:
                desc += f"- {param.name}: {param.description} (default: {param.default})\n"
        return desc

    @property
    def input_schema(self) -> Dict[str, Any]:
        properties = {}
        required = []
        for param in self.config.parameters:
            properties[param.name] = {
                "type": param.type,
                "description": param.description,
                "default": param.default
            }
            if param.required and param.default is None:
                required.append(param.name)
        
        return {
            "type": "object",
            "properties": properties,
            "required": required
        }

    def execute(self, **kwargs) -> Dict[str, Any]:
        raise NotImplementedError

class SQLRiskCheckSkill(BaseTemplateSkill):
    """
    Implementation for 'sql_risk_check' template type.
    Executes a templated SQL query and evaluates risk logic.
    """
    def execute(self, **kwargs) -> Dict[str, Any]:
        # 1. Parameter Validation & Filling Defaults
        params = {}
        for param in self.config.parameters:
            val = kwargs.get(param.name, param.default)
            if val is None and param.required:
                raise ValueError(f"Missing required parameter: {param.name}")
            params[param.name] = val
            
        # 2. Render SQL Template
        if not self.config.sql_template:
            raise ValueError("Configuration missing sql_template")
            
        template = Template(self.config.sql_template)
        rendered_sql = template.render(**params)
        
        # 3. Execute SQL via MCP
        # Using execute_query (safe_query) from MCP
        result = self.mcp_client.call("hpf-db-adapter", "safe_query", {
            "sql": rendered_sql,
            "mask_data": True
        })
        
        if "error" in result:
            return {"error": result["error"], "sql": rendered_sql}
            
        rows = result.get("data", [])
        
        # 4. Evaluate Risk Logic
        risk_result = {
            "risk_detected": False,
            "level": "Safe",
            "message": "No risk detected",
            "details": rows
        }
        
        if self.config.risk_logic:
            logic = self.config.risk_logic
            # Safe evaluation context
            eval_context = {
                "results": rows,
                "len": len,
                "max": max,
                "min": min,
                "sum": sum
            }
            
            try:
                # Evaluate condition
                # Use simple eval or just eval with restricted scope for MVP
                is_risk = eval(logic.condition, {"__builtins__": {}}, eval_context)
                
                if is_risk:
                    risk_result["risk_detected"] = True
                    risk_result["level"] = logic.risk_level
                    # Format message
                    try:
                        risk_result["message"] = eval(f"f'{logic.message}'", {"__builtins__": {}}, eval_context)
                    except:
                        risk_result["message"] = logic.message
            except Exception as e:
                risk_result["error_evaluating_logic"] = str(e)
                
        return risk_result

class SkillFactory:
    """Factory to create skill instances from configuration"""
    @staticmethod
    def create_skill(config_data: Dict) -> BaseSkill:
        # Validate config using Pydantic model
        config = SkillConfiguration(**config_data)
        
        if config.template_type == "sql_risk_check":
            return SQLRiskCheckSkill(config)
        else:
            raise ValueError(f"Unknown template type: {config.template_type}")
