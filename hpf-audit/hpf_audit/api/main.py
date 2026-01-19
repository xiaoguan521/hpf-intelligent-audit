"""
API 主入口
整合所有路由
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import os

# 加载 .env
load_dotenv()

from .routes import agent, skills, knowledge

# 创建 FastAPI 实例
app = FastAPI(
    title="公积金审计 Agent API",
    description="基于 ReAct 框架的智能审计系统",
    version="1.0.0"
)

# CORS 配置
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册路由
app.include_router(agent.router)
app.include_router(skills.router)
app.include_router(knowledge.router)


@app.get("/")
async def root():
    """API 根路由"""
    return {
        "name": "公积金审计 Agent API",
        "version": "1.0.0",
        "endpoints": {
            "agent_chat": "POST /api/agent/chat",
            "run_audit": "POST /api/agent/audit",
            "list_skills": "GET /api/agent/skills",
            "docs": "GET /docs"
        }
    }


@app.get("/health")
async def health_check():
    """健康检查"""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
