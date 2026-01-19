"""
数据库 Schema 加载工具 (Wrapper around hpf_common)
"""
import os
from hpf_common.db.inspector import get_sqlite_schema

def get_schema_context() -> str:
    """获取数据库 Schema 上下文"""
    db_path = os.getenv("DB_PATH", "./housing_provident_fund.db")
    if not os.path.isabs(db_path):
        db_path = os.path.abspath(db_path)
    
    # 使用 Common 组件，指定业务特定的过滤规则
    return get_sqlite_schema(
        db_path=db_path, 
        exclude_tables=['sqlite_%', 'META_%']
    )

def refresh_cache():
    """刷新 Schema 缓存 (占位符)"""
    pass
