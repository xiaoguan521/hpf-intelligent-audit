"""
DuckDB 数据库连接管理模块
========================
- 读取端使用 read_only=True 避免文件锁冲突
- 写入端使用默认连接
- 兼容现有 SQLite API 接口
"""
import duckdb
import os
from contextlib import contextmanager
from typing import Optional, List, Dict, Any

# 从环境变量获取数据库路径
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "./data/warehouse.duckdb")


def get_duckdb_path() -> str:
    """获取 DuckDB 数据库路径"""
    return DUCKDB_PATH


@contextmanager
def get_read_connection():
    """
    获取只读 DuckDB 连接
    用于所有读取操作（API 查询、AI 分析等）
    
    使用 read_only=True 避免与写入端的文件锁冲突
    """
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def get_write_connection():
    """
    获取可写 DuckDB 连接
    仅用于 ETL 写入、数据导入等操作
    """
    conn = duckdb.connect(DUCKDB_PATH)
    try:
        yield conn
    finally:
        conn.close()


def execute_query(sql: str, params: tuple = None) -> List[Dict[str, Any]]:
    """
    执行查询并返回字典列表
    
    Args:
        sql: SQL 查询语句
        params: 查询参数
    
    Returns:
        查询结果列表
    """
    with get_read_connection() as conn:
        if params:
            result = conn.execute(sql, params)
        else:
            result = conn.execute(sql)
        
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        
        return [dict(zip(columns, row)) for row in rows]


def execute_query_with_pagination(
    sql: str,
    page: int = 1,
    per_page: int = 10,
    params: tuple = None
) -> Dict[str, Any]:
    """
    执行分页查询
    
    Args:
        sql: SQL 查询语句（不含 LIMIT/OFFSET）
        page: 页码（从 1 开始）
        per_page: 每页条数
        params: 查询参数
    
    Returns:
        包含 items, total, page, per_page 的字典
    """
    with get_read_connection() as conn:
        # 获取总数
        count_sql = f"SELECT COUNT(*) FROM ({sql}) AS _count"
        if params:
            total = conn.execute(count_sql, params).fetchone()[0]
        else:
            total = conn.execute(count_sql).fetchone()[0]
        
        # 分页查询
        offset = (page - 1) * per_page
        page_sql = f"{sql} LIMIT {per_page} OFFSET {offset}"
        
        if params:
            result = conn.execute(page_sql, params)
        else:
            result = conn.execute(page_sql)
        
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        items = [dict(zip(columns, row)) for row in rows]
        
        return {
            "items": items,
            "total": total,
            "page": page,
            "per_page": per_page
        }


def get_table_columns(table_name: str) -> List[str]:
    """获取表的列名列表"""
    with get_read_connection() as conn:
        result = conn.execute(f"DESCRIBE {table_name}")
        return [row[0] for row in result.fetchall()]


def table_exists(table_name: str) -> bool:
    """检查表是否存在"""
    with get_read_connection() as conn:
        result = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            (table_name,)
        )
        return result.fetchone()[0] > 0
