from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field

class SkillParameter(BaseModel):
    """Template Parameter Definition"""
    name: str = Field(..., description="Parameter name (used in Jinja2)")
    type: str = Field(..., description="Data type: string, number, boolean, list")
    default: Optional[Any] = None
    description: str = ""
    required: bool = True

class RiskLogic(BaseModel):
    """Logic to determine risk level"""
    risk_level: str = Field(..., pattern="^(Low|Medium|High)$")
    condition: str = Field(..., description="Python expression evaluating result")
    message: str = Field(..., description="Python formatting string for result message")

class SkillMeta(BaseModel):
    """Metadata for the skill"""
    name: str
    description: str
    tags: List[str] = []
    related_skills: List[str] = []  # List of related skill_ids

class SkillConfiguration(BaseModel):
    """
    Root configuration object for a generated skill.
    This corresponds to the YAML content.
    """
    skill_id: str
    template_type: str = Field(..., description="e.g., sql_risk_check")
    meta: SkillMeta
    parameters: List[SkillParameter] = []
    
    # Template-specific fields (used by SQLRiskCheckSkill)
    sql_template: Optional[str] = None
    risk_logic: Optional[RiskLogic] = None
    
    # Flattened access to related skills (optional convenience)
    @property
    def related_skills(self) -> List[str]:
        return self.meta.related_skills
    
    class Config:
        extra = "ignore" # Allow extra fields for future compatibility
