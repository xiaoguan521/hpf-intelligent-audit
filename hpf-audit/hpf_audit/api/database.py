"""
统一的数据库配置模块
提供数据库路径配置和连接管理
"""
import sqlite3
import os
from typing import Optional

# 从环境变量获取数据库路径，默认为相对路径
DB_PATH = os.path.abspath(os.getenv("DB_PATH", "./housing_provident_fund.db"))


def get_db_connection(readonly: bool = False) -> sqlite3.Connection:
    """
    获取数据库连接
    
    Args:
        readonly: 是否以只读模式打开数据库
        
    Returns:
        sqlite3.Connection: 数据库连接对象
    """
    if readonly:
        # 只读模式，使用 URI 模式打开
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    else:
        # 读写模式
        conn = sqlite3.connect(DB_PATH)
    
    # 设置 row_factory 以便返回字典格式
    conn.row_factory = sqlite3.Row
    return conn
