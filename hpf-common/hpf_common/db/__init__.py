"""
数据库连接管理器
"""
from contextlib import contextmanager
from typing import Generator, Union
import os


class DBManager:
    """
    统一的数据库连接管理器
    
    支持: SQLite, DuckDB, Oracle
    
    使用示例:
        from core.db import DBManager
        
        # SQLite
        with DBManager.connect('sqlite', path='data.db') as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM table")
        
        # DuckDB
        with DBManager.connect('duckdb', path='warehouse.duckdb') as conn:
            df = conn.execute("SELECT * FROM table").df()
        
        # Oracle
        with DBManager.connect('oracle', dsn='...') as engine:
            with engine.connect() as conn:
                result = conn.execute("SELECT * FROM table")
    """
    
    @staticmethod
    @contextmanager
    def connect(db_type: str, **kwargs) -> Generator:
        """
        通用连接接口
        
        Args:
            db_type: 数据库类型 ('sqlite', 'duckdb', 'oracle')
            **kwargs: 连接参数
        
        Yields:
            数据库连接对象
        """
        if db_type == 'sqlite':
            conn = DBManager._connect_sqlite(**kwargs)
        elif db_type == 'duckdb':
            conn = DBManager._connect_duckdb(**kwargs)
        elif db_type == 'oracle':
            conn = DBManager._connect_oracle(**kwargs)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
        
        try:
            yield conn
        finally:
            if hasattr(conn, 'close'):
                conn.close()
            elif hasattr(conn, 'dispose'):
                conn.dispose()
    
    @staticmethod
    def _connect_sqlite(**kwargs):
        """连接 SQLite"""
        import sqlite3
        
        path = kwargs.get('path', 'housing_provident_fund.db')
        return sqlite3.connect(path)
    
    @staticmethod
    def _connect_duckdb(**kwargs):
        """连接 DuckDB"""
        try:
            import duckdb
        except ImportError:
            raise ImportError("请安装 duckdb: pip install duckdb")
        
        path = kwargs.get('path', 'data/warehouse.duckdb')
        read_only = kwargs.get('read_only', False)
        
        return duckdb.connect(path, read_only=read_only)
    
    @staticmethod
    def _connect_oracle(**kwargs):
        """连接 Oracle (返回 SQLAlchemy Engine)"""
        try:
            from sqlalchemy import create_engine
            import oracledb
        except ImportError:
            raise ImportError("请安装依赖: pip install sqlalchemy oracledb")
        
        # 从环境变量或参数获取连接信息
        user = kwargs.get('user') or os.getenv('ORACLE_USER')
        password = kwargs.get('password') or os.getenv('ORACLE_PASSWORD')
        host = kwargs.get('host') or os.getenv('ORACLE_HOST', 'localhost')
        port = kwargs.get('port') or os.getenv('ORACLE_PORT', '1521')
        service = kwargs.get('service') or os.getenv('ORACLE_SERVICE', 'ORCL')
        
        dsn = f"{user}:{password}@{host}:{port}/{service}"
        engine = create_engine(f"oracle+oracledb://{dsn}")
        
        return engine


__all__ = ["DBManager"]
