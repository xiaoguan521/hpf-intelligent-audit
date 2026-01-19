"""
LangGraph Agent (新版)
使用LangGraph替代自研ReAct引擎
"""
from typing import List, Dict, Any, Optional
import json
import os

try:
    from langgraph.prebuilt import create_react_agent
    from langchain_core.messages import SystemMessage, HumanMessage
    from langchain_openai import ChatOpenAI
    LANGGRAPH_AVAILABLE = True
except ImportError:
    LANGGRAPH_AVAILABLE = False
    print("Warning: LangGraph not available")

from hpf_audit.skills.langchain_tools import ALL_TOOLS


class LangGraphAgent:
    """基于LangGraph的Agent引擎"""
    
    def __init__(
        self,
        llm_client: Optional[Any] = None,  # 为了兼容性保留，但不使用
        max_iterations: int = 5,
        verbose: bool = True
    ):
        if not LANGGRAPH_AVAILABLE:
            raise ImportError("LangGraph is required. Run: pip install langgraph")
        
        self.verbose = verbose
        self.max_iterations = max_iterations
        
        # 使用LangChain原生LLM (需要有bind_tools方法)
        # 从环境变量读取配置
        provider = os.getenv("DEFAULT_LLM_PROVIDER", "nvidia")
        api_key = None
        base_url = None
        model = None
        
        if provider == "nvidia":
            api_key = os.getenv("NVIDIA_API_KEY")
            base_url = os.getenv("NVIDIA_BASE_URL", "https://integrate.api.nvidia.com/v1")
            model = os.getenv("DEFAULT_LLM_MODEL", "meta/llama-3.1-70b-instruct")
        elif provider == "openai":
            api_key = os.getenv("OPENAI_API_KEY")
            base_url = os.getenv("OPENAI_BASE_URL")
            model = os.getenv("OPENAI_MODEL") or os.getenv("DEFAULT_LLM_MODEL", "gpt-4o-mini")
        elif provider == "deepseek":
            api_key = os.getenv("DEEPSEEK_API_KEY")
            base_url = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1")
            model = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")
        elif provider == "cerebras":
            api_key = os.getenv("CEREBRAS_API_KEY")
            base_url = os.getenv("CEREBRAS_BASE_URL", "https://api.cerebras.ai/v1")
            model = os.getenv("CEREBRAS_MODEL", "llama3.1-70b")
        else:
            # 默认使用nvidia
            api_key = os.getenv("NVIDIA_API_KEY")
            base_url = os.getenv("NVIDIA_BASE_URL", "https://integrate.api.nvidia.com/v1")
            model = os.getenv("DEFAULT_LLM_MODEL", "meta/llama-3.1-70b-instruct")
        
        # 创建ChatOpenAI实例
        if not api_key:
            raise ValueError(f"未找到{provider}的API Key，请检查.env文件")
        
        self.llm = ChatOpenAI(
            api_key=api_key,
            base_url=base_url,
            model=model,
            temperature=0.7
        )
        
        # 初始化向量存储(用于Skill推荐) - 可选
        self.vector_store = None
        try:
            from hpf_audit.knowledge.vector_store import VectorStoreManager
            self.vector_store = VectorStoreManager()
            if self.verbose:
                print("✅ Skill向量检索已启用")
        except Exception as e:
            if self.verbose:
                print(f"⚠️ Skill向量检索未启用: {e}")
        
        # 创建Agent
        self.agent = self._create_agent()
    
    def _create_agent(self):
        """创建LangGraph ReAct Agent"""
        
        # 创建Agent (不使用state_modifier)
        agent = create_react_agent(
            model=self.llm,
            tools=ALL_TOOLS
        )
        
        return agent
    
    def run(self, user_query: str) -> Dict[str, Any]:
        """
        执行Agent推理
        
        Args:
            user_query: 用户问题
        
        Returns:
            {
                "answer": "最终答案",
                "reasoning_chain": [...],
                "iterations": 3
            }
        """
        # 1. 推荐相关Skills (可选)
        recommended_skills = []
        if self.vector_store:
            try:
                recommended_skills = self.vector_store.search_skills(user_query, top_k=3)
                
                if self.verbose and recommended_skills:
                    print("\n🔍 推荐Skills:")
                    for skill in recommended_skills:
                        print(f"  - {skill['name']} (相关度: {skill['score']:.2f})")
            except Exception as e:
                if self.verbose:
                    print(f"⚠️ Skill推荐失败: {e}")
        
        # 2. 执行Agent (将system prompt作为第一条消息)
        system_prompt = f"""你是公积金业务审计专家。

**你的任务**:
用户会提出审计相关问题，你需要：
1. 理解问题并选择合适的审计工具
2. 调用工具获取数据
3. 分析结果并给出专业建议

**可用工具**:
- withdrawal_audit: 提取审计
- loan_compliance: 贷款合规
- internal_control_audit: 内控审计
- organization_audit: 单位审计
- data_analysis: 数据分析

**重要规则**:
- 优先使用工具而非臆测
- 每次只调用一个工具
- 基于工具返回的数据给出结论
- 如果工具失败，解释原因并尝试其他方法
- 控制在{self.max_iterations}轮内完成任务
"""
        
        try:
            result = self.agent.invoke({
                "messages": [
                   SystemMessage(content=system_prompt),
                    HumanMessage(content=user_query)
                ]
            })
            
            # 3. 提取结果
            messages = result.get("messages", [])
            answer = messages[-1].content if messages else "未获取到回答"
            
            return {
                "answer": answer,
                "reasoning_chain": self._extract_chain(messages),
                "iterations": len([m for m in messages if hasattr(m, 'type') and m.type == "ai"]),
                "recommended_skills": recommended_skills
            }
        except Exception as e:
            import traceback
            error_msg = f"Agent执行失败: {str(e)}\n{traceback.format_exc()}"
            if self.verbose:
                print(f"❌ {error_msg}")
            
            return {
                "answer": f"执行出错: {str(e)}",
                "reasoning_chain": [],
                "iterations": 0,
                "error": error_msg
            }
    
    def _extract_chain(self, messages) -> List[Dict]:
        """从消息中提取推理链"""
        chain = []
        iteration = 0
        
        for msg in messages:
            msg_type = getattr(msg, 'type', 'unknown')
            
            if msg_type == "ai":
                iteration += 1
                chain.append({
                    "iteration": iteration,
                    "thought": msg.content,
                    "type": "reasoning"
                })
            elif msg_type == "tool":
                chain.append({
                    "iteration": iteration,
                    "type": "tool_result",
                    "tool": getattr(msg, 'name', 'unknown'),
                    "result": msg.content
                })
        
        return chain
