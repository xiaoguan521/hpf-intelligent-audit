"""
数据库 Schema 检查工具
"""
import sqlite3
from typing import List, Optional

def get_sqlite_schema(db_path: str, exclude_tables: Optional[List[str]] = None) -> str:
    """
    获取 SQLite 数据库 Schema
    
    Args:
        db_path: 数据库路径
        exclude_prefixes: 要排除的表名前缀列表 (e.g. ['sqlite_', 'META_'])
    
    Returns:
        Schema 描述字符串
    """
    if exclude_tables is None:
        exclude_tables = ['sqlite_%']
        
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        cursor = conn.cursor()
        
        # 构建 SQL 查询
        query = "SELECT name, sql FROM sqlite_master WHERE type='table'"
        params = []
        
        if exclude_tables:
            conditions = []
            for pattern in exclude_tables:
                conditions.append("name NOT LIKE ?")
                params.append(pattern)
            if conditions:
                query += " AND " + " AND ".join(conditions)
                
        cursor.execute(query, params)
        tables = cursor.fetchall()
        
        schema_text = []
        for name, sql in tables:
            if sql:
                schema_text.append(f"Table: {name}\nDDL: {sql}\n")
        
        conn.close()
        return "\n".join(schema_text)
    except Exception as e:
        print(f"Error loading schema: {e}")
        return ""
