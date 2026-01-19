from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional, List, Any, Dict, Union
import duckdb
import os
import re

router = APIRouter()

class SqlRequest(BaseModel):
    sql: str
    page: int = 1
    perPage: int = 10

class SqlResponse(BaseModel):
    status: int
    msg: str
    data: Optional[Dict[str, Any]] = None

def get_duckdb_path():
    """获取 DuckDB 数据库路径"""
    return os.getenv('DUCKDB_PATH', './data/warehouse.duckdb')

@router.post("/execute", response_model=SqlResponse)
async def execute_sql(request: SqlRequest):
    """
    执行 SQL 语句（DuckDB 版本）
    
    - 自动识别 SELECT 语句并进行分页处理
    - 支持 DML/DDL 语句执行
    - 返回动态列信息供前端渲染
    - 读取端使用 read_only=True 避免文件锁冲突
    """
    sql = request.sql.strip()
    if not sql:
        return SqlResponse(status=1, msg="SQL 语句不能为空")

    # 简单判断是否为查询语句
    is_select = sql.upper().startswith("SELECT")
    
    db_path = get_duckdb_path()
    conn = None
    
    try:
        if is_select:
            # 读取操作使用只读连接
            conn = duckdb.connect(db_path, read_only=True)
            
            # 1. 获取总记录数 (Wrap original query)
            clean_sql = sql.rstrip(';')
            count_sql = f"SELECT COUNT(*) as total FROM ({clean_sql}) as _count_wrapper"
            
            try:
                result = conn.execute(count_sql)
                total = result.fetchone()[0]
            except Exception as e:
                conn.close()
                return SqlResponse(status=1, msg=f"查询总数失败: {str(e)}")

            # 2. 分页查询
            offset = (request.page - 1) * request.perPage
            page_sql = f"{clean_sql} LIMIT {request.perPage} OFFSET {offset}"
            
            result = conn.execute(page_sql)
            
            # 3. 提取列信息
            columns = []
            if result.description:
                columns = [col[0] for col in result.description]
            
            # 4. 转换数据
            rows = result.fetchall()
            data_list = [dict(zip(columns, row)) for row in rows]
            
            conn.close()
            
            return SqlResponse(
                status=0,
                msg="ok",
                data={
                    "type": "dql",
                    "rows": data_list,
                    "columns": columns,
                    "total": total,
                    "page": request.page,
                    "perPage": request.perPage
                }
            )
            
        else:
            # DML / DDL 需要写入权限
            conn = duckdb.connect(db_path)
            conn.execute(sql)
            # DuckDB 没有直接的 rowcount，这里简化处理
            affected = -1  # DuckDB 不总是返回受影响行数
            conn.close()
            
            return SqlResponse(
                status=0,
                msg="执行成功",
                data={
                    "type": "dml",
                    "affected_rows": affected
                }
            )

    except duckdb.Error as e:
        if conn:
            conn.close()
        return SqlResponse(status=1, msg=f"SQL 执行错误: {str(e)}")
    except Exception as e:
        if conn:
            conn.close()
        return SqlResponse(status=1, msg=f"系统错误: {str(e)}")
