"""
Agent API 路由
提供 Agent 对话和推理接口
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

# ✅ 从包根导入
from hpf_audit.agent.react_engine import ReActAgent
from hpf_audit.skills import get_skill_registry
from hpf_common.llm import LLMClient

router = APIRouter(prefix="/api/agent", tags=["agent"])


class ChatRequest(BaseModel):
    """对话请求"""
    query: str
    session_id: Optional[str] = None
    max_iterations: Optional[int] = 5
    use_langgraph: bool = True  # ✨ 新增：是否使用LangGraph Agent


class ChatResponse(BaseModel):
    """对话响应"""
    answer: str
    reasoning_chain: list
    iterations: int
    agent_type: Optional[str] = "langgraph"  # ✨ 新增：标识使用的Agent类型


@router.post("/chat", response_model=ChatResponse)
async def agent_chat(request: ChatRequest):
    """
    Agent 智能对话接口（支持多步推理）
    
    支持两种Agent引擎：
    - LangGraph (推荐): 基于LangChain，代码更简洁
    - ReAct (Fallback): 自研引擎，稳定可靠
    
    示例请求：
    ```json
    {
      "query": "检查最近有没有购房提取频次异常的账户",
      "use_langgraph": true
    }
    ```
    """
    try:
        if request.use_langgraph:
            # 使用LangGraph Agent (新版)
            from hpf_audit.agent.engine_v2 import LangGraphAgent
            
            agent = LangGraphAgent(
                max_iterations=request.max_iterations or 5,
                verbose=True
            )
            agent_type = "langgraph"
        else:
            # 使用ReAct Agent (旧版 - fallback)
            llm = LLMClient()
            skill_registry = get_skill_registry()
            skills = skill_registry.get_all_skills()
            
            agent = ReActAgent(
                llm_client=llm,
                skills=skills,
                max_iterations=request.max_iterations or 5,
                verbose=True
            )
            agent_type = "react"
        
        # 执行推理
        result = agent.run(request.query)
        
        return ChatResponse(
            answer=result["answer"],
            reasoning_chain=result["reasoning_chain"],
            iterations=result["iterations"],
            agent_type=agent_type
        )
    
    except Exception as e:
        import traceback
        error_detail = traceback.format_exc()
        raise HTTPException(
            status_code=500, 
            detail=f"Agent 执行失败: {str(e)}\n{error_detail}"
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Agent 执行失败: {str(e)}")


@router.post("/audit")
async def run_audit(request: dict):
    """
    直接调用审计 Skill（不经过 Agent 推理）
    
    示例请求：
    ```json
    {
      "skill": "withdrawal_audit",
      "params": {
        "check_type": "frequent_purchase"
      }
    }
    ```
    """
    skill_name = request.get("skill")
    params = request.get("params", {})
    
    skill_registry = get_skill_registry()
    skill = skill_registry.get_skill(skill_name)
    
    if not skill:
        raise HTTPException(
            status_code=404,
            detail=f"Skill '{skill_name}' 不存在"
        )
    
    try:
        result = skill.execute(**params)
        return {
            "status": "success",
            "skill": skill_name,
            "result": result
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Skill 执行失败: {str(e)}"
        )


@router.get("/skills")
async def list_skills():
    """列出所有可用的 Skills"""
    skill_registry = get_skill_registry()
    return {
        "skills": skill_registry.list_skills()
    }


class SkillGenerateRequest(BaseModel):
    """Skill生成请求"""
    requirement: str
    mode: Optional[str] = "shadow"  # shadow 或 active


class SkillGenerateResponse(BaseModel):
    """Skill生成响应"""
    success: bool
    skill_id: Optional[str] = None
    yaml_config: Optional[str] = None
    error: Optional[str] = None


@router.post("/generate_skill", response_model=SkillGenerateResponse)
async def generate_skill(request: SkillGenerateRequest):
    """
    AI自动生成Skill配置
    
    示例请求：
    ```json
    {
      "requirement": "创建逾期风险监测Skill，查询所有逾期未清的贷款...",
      "mode": "shadow"
    }
    ```
    """
    try:
        from hpf_audit.skills.generator import SkillGenerator
        
        # 创建生成器
        generator = SkillGenerator()
        
        # 生成配置
        config_yaml = generator.generate(request.requirement)
        
        if not config_yaml:
            return SkillGenerateResponse(
                success=False,
                error="生成失败：无法生成有效配置"
            )
        
        # 提取skill_id（用于保存）
        import yaml
        try:
            config_dict = yaml.safe_load(config_yaml)
            skill_id = config_dict.get('skill_id', 'generated_skill')
        except:
            skill_id = 'generated_skill'
        
        # 保存到数据库（Shadow Mode或Active Mode）
        saved = generator.save_to_db(
            config_yaml=config_yaml,
            requirement=request.requirement,
            is_active=1 if request.mode == "active" else 0
        )
        
        if not saved:
            return SkillGenerateResponse(
                success=False,
                yaml_config=config_yaml,
                error="配置生成成功但保存失败"
            )
        
        return SkillGenerateResponse(
            success=True,
            skill_id=skill_id,
            yaml_config=config_yaml,
            error=None
        )
    
    except Exception as e:
        import traceback
        return SkillGenerateResponse(
            success=False,
            error=f"生成失败: {str(e)}\n{traceback.format_exc()}"
        )

