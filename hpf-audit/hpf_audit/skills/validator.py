import yaml
from typing import Dict, Any, Tuple
from jinja2 import Template
from hpf_audit.skills.models import SkillConfiguration

class ConfigurationValidator:
    """
    Validates Skill Configuration YAML/JSON.
    Checks:
    1. Structure (Pydantic)
    2. Jinja2 Syntax (SQL Template)
    3. Basic SQL Safety (No DML)
    """

    @staticmethod
    def validate_yaml(yaml_content: str) -> Tuple[bool, str, Dict]:
        """
        Validate YAML content.
        Returns: (is_valid, error_message, parsed_dict)
        """
        try:
            # 1. Parse YAML
            config_dict = yaml.safe_load(yaml_content)
            if not isinstance(config_dict, dict):
                return False, "Root must be a dictionary", {}
            
            # 2. Pydantic Validation
            config = SkillConfiguration(**config_dict)
            
            # 3. Template Type Specific Validation
            if config.template_type == "sql_risk_check":
                return ConfigurationValidator._validate_sql_risk_check(config)
                
            return True, "Valid", config_dict
            
        except yaml.YAMLError as e:
            return False, f"YAML Syntax Error: {e}", {}
        except Exception as e:
            return False, f"Validation Error: {e}", {}

    @staticmethod
    def _validate_sql_risk_check(config: SkillConfiguration) -> Tuple[bool, str, Dict]:
        if not config.sql_template:
            return False, "Missing sql_template", {}
            
        # 4. Check Jinja2 Syntax
        try:
            Template(config.sql_template)
        except Exception as e:
            return False, f"Jinja2 Template Error: {e}", {}
            
        # 5. Basic SQL Check (Static)
        sql_upper = config.sql_template.upper()
        forbidden = ["DROP ", "DELETE ", "UPDATE ", "INSERT ", "ALTER ", "TRUNCATE "]
        for word in forbidden:
            if word in sql_upper:
                return False, f"Potential unsafe SQL detected: {word}", {}
                
        return True, "Valid", config.dict()
