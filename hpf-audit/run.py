#!/usr/bin/env python3
"""
hpf-audit 启动脚本
"""
import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "hpf_audit.api.main:app",  # ✅ 使用import string而非直接导入
        host="0.0.0.0",
        port=8000,
        reload=True  # 开发模式下自动重载
    )
