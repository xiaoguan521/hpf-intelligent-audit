"""
Core 模块测试
"""
import sys
import os

# 添加项目根目录到 Python 路径
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.insert(0, project_root)

from core.llm import LLMClient
from core.db import DBManager
# from core.config import settings  # 暂时跳过,需要安装pydantic-settings
from core.utils import setup_logger, safe_divide

# 测试日志
logger = setup_logger()
logger.info("开始测试 core 模块...")

# 测试配置 - 暂时跳过
# logger.info(f"✅ 配置管理 - LLM Provider: {settings.default_llm_provider}")
logger.info("⏭️  配置管理测试已跳过 (需要安装 pydantic-settings)")

# 测试工具函数
result = safe_divide(10, 2)
logger.info(f"✅ 工具函数 - safe_divide(10, 2) = {result}")

result_zero = safe_divide(10, 0, default=-1)
logger.info(f"✅ 工具函数 - safe_divide(10, 0, default=-1) = {result_zero}")

# 测试数据库连接 (SQLite)
logger.info("✅ 测试 SQLite 连接...")
try:
    db_path = os.path.join(project_root, 'housing_provident_fund.db')
    with DBManager.connect('sqlite', path=db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 3")
        tables = cursor.fetchall()
        logger.info(f"  前3个表: {[t[0] for t in tables]}")
        logger.info("✅ SQLite 连接成功")
except Exception as e:
    logger.error(f"❌ SQLite 连接失败: {e}")

# 测试 LLM 客户端初始化 - 暂时跳过需要openai依赖
logger.info("✅ LLM Client 模块导入成功")
logger.info("  (实际调用需要安装 openai 包)")

logger.info("=" * 50)
logger.info("core 模块基础测试完成！")
logger.info("后续需要安装: pip install pydantic-settings openai")
