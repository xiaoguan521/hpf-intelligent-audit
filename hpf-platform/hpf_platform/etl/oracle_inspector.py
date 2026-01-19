"""
Oracle 表元数据检查器
====================
获取 Oracle 表的 DDL、大小、行数、分区信息等元数据
用于智能同步策略推荐
"""
import logging
from typing import Dict, List, Any, Optional
from sqlalchemy import create_engine, text
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class TableMetadata:
    """表元数据"""
    table_name: str
    schema: str
    row_count: int
    size_mb: float
    is_partitioned: bool
    partition_count: int
    partitions: List[str]
    primary_key: Optional[str]
    columns: List[Dict[str, str]]
    incremental_candidates: List[Dict]  # 可能的增量字段 [{"name": "col", "non_null_pct": 95.5}, ...]
    ddl: str


class OracleInspector:
    """Oracle 表元数据检查器"""
    
    def __init__(self, connection_string: str):
        """
        初始化检查器
        
        Args:
            connection_string: Oracle SQLAlchemy 连接字符串
        """
        self.connection_string = connection_string
        self._engine = None
    
    @property
    def engine(self):
        """懒加载数据库引擎"""
        if self._engine is None:
            self._engine = create_engine(
                self.connection_string,
                pool_size=2,
                max_overflow=1,
                pool_pre_ping=True,
                echo=False
            )
        return self._engine
    
    def test_connection(self) -> bool:
        """测试数据库连接"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1 FROM DUAL"))
            return True
        except Exception as e:
            logger.error(f"Oracle 连接失败: {e}")
            return False
    
    def get_all_tables(self, schema: str) -> List[str]:
        """
        获取 schema 下所有表名
        
        Args:
            schema: Schema 名称
            
        Returns:
            表名列表
        """
        query = """
            SELECT TABLE_NAME 
            FROM ALL_TABLES 
            WHERE OWNER = UPPER(:schema)
            ORDER BY TABLE_NAME
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {"schema": schema})
            return [row[0] for row in result.fetchall()]
    
    def get_table_row_count(self, table_name: str, schema: str) -> int:
        """获取表行数（使用统计信息，快速但可能不精确）"""
        # 先尝试从统计信息获取（快速）
        query = """
            SELECT NUM_ROWS 
            FROM ALL_TABLES 
            WHERE TABLE_NAME = UPPER(:table_name) 
            AND OWNER = UPPER(:schema)
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {
                "table_name": table_name,
                "schema": schema
            })
            row = result.fetchone()
            if row and row[0] is not None:
                return int(row[0])
        
        # 统计信息不可用，使用 COUNT（慢）
        full_table = f"{schema}.{table_name}"
        query = f"SELECT COUNT(*) FROM {full_table}"
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            return result.fetchone()[0]
    
    def get_table_size_mb(self, table_name: str, schema: str) -> float:
        """获取表大小（MB）"""
        query = """
            SELECT NVL(SUM(bytes) / 1024 / 1024, 0) as size_mb
            FROM ALL_SEGMENTS
            WHERE SEGMENT_NAME = UPPER(:table_name)
            AND OWNER = UPPER(:schema)
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {
                "table_name": table_name,
                "schema": schema
            })
            row = result.fetchone()
            return float(row[0]) if row else 0.0
    
    def get_partition_info(self, table_name: str, schema: str) -> Dict[str, Any]:
        """获取分区信息"""
        query = """
            SELECT PARTITION_NAME, PARTITION_POSITION
            FROM ALL_TAB_PARTITIONS
            WHERE TABLE_NAME = UPPER(:table_name)
            AND TABLE_OWNER = UPPER(:schema)
            ORDER BY PARTITION_POSITION
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {
                "table_name": table_name,
                "schema": schema
            })
            partitions = [row[0] for row in result.fetchall()]
            
            return {
                "is_partitioned": len(partitions) > 0,
                "partition_count": len(partitions),
                "partitions": partitions
            }
    
    def get_columns(self, table_name: str, schema: str) -> List[Dict[str, str]]:
        """获取表列信息"""
        query = """
            SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE
            FROM ALL_TAB_COLUMNS
            WHERE TABLE_NAME = UPPER(:table_name)
            AND OWNER = UPPER(:schema)
            ORDER BY COLUMN_ID
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {
                "table_name": table_name,
                "schema": schema
            })
            return [{
                "name": row[0],
                "type": row[1],
                "length": row[2],
                "nullable": row[3] == 'Y'
            } for row in result.fetchall()]
    
    def get_primary_key(self, table_name: str, schema: str) -> Optional[str]:
        """获取主键列名"""
        query = """
            SELECT cols.COLUMN_NAME
            FROM ALL_CONSTRAINTS cons
            JOIN ALL_CONS_COLUMNS cols 
                ON cons.CONSTRAINT_NAME = cols.CONSTRAINT_NAME 
                AND cons.OWNER = cols.OWNER
            WHERE cons.CONSTRAINT_TYPE = 'P'
            AND cons.TABLE_NAME = UPPER(:table_name)
            AND cons.OWNER = UPPER(:schema)
            ORDER BY cols.POSITION
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {
                "table_name": table_name,
                "schema": schema
            })
            rows = result.fetchall()
            if rows:
                # 返回第一个主键列（如果是复合主键）
                return rows[0][0]
            return None
    
    def find_incremental_candidates(self, table_name: str, schema: str) -> List[Dict]:
        """
        找出可能的增量字段候选（包含非空率检查）
        优先选择: ID、UPDATE_TIME、CREATE_TIME 等
        
        Returns:
            List[Dict]: [{"name": "col", "type": "NUMBER", "score": 10, "non_null_pct": 95.5}, ...]
        """
        columns = self.get_columns(table_name, schema)
        full_table = f"{schema}.{table_name}"
        candidates = []
        
        # 优先级关键词
        priority_keywords = [
            ('ID', 10),
            ('UPDATE_TIME', 9),
            ('MODIFY_TIME', 9),
            ('UPDATE_DATE', 8),
            ('CREATE_TIME', 7),
            ('CREATE_DATE', 7),
            ('SEQ', 6),
            ('SEQUENCE', 6),
        ]
        
        for col in columns:
            col_name = col['name'].upper()
            col_type = col['type'].upper()
            
            # 数字类型或日期类型可作为增量字段
            if col_type in ('NUMBER', 'INTEGER', 'DECIMAL', 'DATE', 'TIMESTAMP'):
                score = 0
                for keyword, weight in priority_keywords:
                    if keyword in col_name:
                        score = max(score, weight)
                
                if score > 0 or col_type in ('DATE', 'TIMESTAMP'):
                    # 检查非空率
                    non_null_pct = 100.0  # 默认假设非空
                    try:
                        query = f"""
                            SELECT ROUND(COUNT({col_name}) * 100.0 / NULLIF(COUNT(*), 0), 2)
                            FROM {full_table}
                        """
                        with self.engine.connect() as conn:
                            result = conn.execute(text(query)).fetchone()
                            non_null_pct = float(result[0]) if result and result[0] else 0.0
                    except Exception as e:
                        logger.debug(f"无法检查列 {col_name} 非空率: {e}")
                    
                    candidates.append({
                        "name": col_name,
                        "type": col_type,
                        "score": score,
                        "non_null_pct": non_null_pct
                    })
        
        # 按得分排序
        candidates.sort(key=lambda x: (-x['score'], -x['non_null_pct']))
        return candidates[:5]  # 返回前5个
    
    def get_table_ddl(self, table_name: str, schema: str) -> str:
        """获取表 DDL（简化版，基于列信息构建）"""
        columns = self.get_columns(table_name, schema)
        pk = self.get_primary_key(table_name, schema)
        
        if not columns:
            return f"-- 无法获取 {schema}.{table_name} 的 DDL"
        
        col_defs = []
        for col in columns:
            nullable = "" if col['nullable'] else " NOT NULL"
            col_def = f"  {col['name']} {col['type']}"
            if col['type'] in ('VARCHAR2', 'CHAR', 'NVARCHAR2'):
                col_def += f"({col['length']})"
            col_def += nullable
            col_defs.append(col_def)
        
        ddl = f"CREATE TABLE {schema}.{table_name} (\n"
        ddl += ",\n".join(col_defs)
        if pk:
            ddl += f",\n  CONSTRAINT PK_{table_name} PRIMARY KEY ({pk})"
        ddl += "\n);"
        
        return ddl
    
    def get_table_metadata(self, table_name: str, schema: str) -> TableMetadata:
        """
        获取表的完整元数据
        
        Args:
            table_name: 表名
            schema: Schema 名
            
        Returns:
            TableMetadata 对象
        """
        # 1. 获取基本信息（如果这都失败了，那确实没法同步）
        try:
            row_count = self.get_table_row_count(table_name, schema)
        except Exception as e:
            logger.warning(f"无法获取表 {table_name} 行数: {e}")
            row_count = 0
            
        # 2. 获取表大小（非关键）
        try:
            size_mb = self.get_table_size_mb(table_name, schema)
        except Exception as e:
            msg = str(e)
            if "ORA-00942" in msg:
                # 权限不足无法访问 segment 视图，这是常见情况，无需惊慌
                logger.info(f"跳过表 {table_name} 大小检查 (权限限制)")
            else:
                logger.warning(f"无法获取表 {table_name} 大小: {e}")
            size_mb = 0.0
            
        # 3. 获取分区信息（重要但不应阻塞）
        try:
            partition_info = self.get_partition_info(table_name, schema)
        except Exception as e:
            logger.warning(f"无法获取表 {table_name} 分区信息: {e}")
            partition_info = {"is_partitioned": False, "partition_count": 0, "partitions": []}

        # 4. 获取列和主键
        try:
            pk = self.get_primary_key(table_name, schema)
            columns = self.get_columns(table_name, schema)
            incremental_candidates = self.find_incremental_candidates(table_name, schema)
            ddl = self.get_table_ddl(table_name, schema)
        except Exception as e:
            logger.warning(f"无法获取表 {table_name} 结构信息: {e}")
            # 结构信息失基本没法同步，还是抛出异常比较合适，或者返回最小可用对象
            raise e
        
        return TableMetadata(
            table_name=table_name,
            schema=schema,
            row_count=row_count,
            size_mb=size_mb,
            is_partitioned=partition_info["is_partitioned"],
            partition_count=partition_info["partition_count"],
            partitions=partition_info["partitions"],
            primary_key=pk,
            columns=columns,
            incremental_candidates=incremental_candidates,
            ddl=ddl
        )
    
    def get_multiple_tables_metadata(
        self, 
        table_names: List[str], 
        schema: str,
        progress_callback=None
    ) -> List[TableMetadata]:
        """
        获取多个表的元数据
        
        Args:
            table_names: 表名列表，["*"] 表示所有表
            schema: Schema 名
            progress_callback: 进度回调函数 (current, total, table_name)
            
        Returns:
            TableMetadata 列表
        """
        # 处理 * 通配符
        if table_names == ["*"] or (len(table_names) == 1 and table_names[0] == "*"):
            table_names = self.get_all_tables(schema)
        
        results = []
        total = len(table_names)
        
        for i, table_name in enumerate(table_names):
            if progress_callback:
                progress_callback(i + 1, total, table_name)
            
            try:
                metadata = self.get_table_metadata(table_name, schema)
                results.append(metadata)
            except Exception as e:
                logger.warning(f"获取表 {table_name} 元数据失败: {e}")
        
        return results
    
    def close(self):
        """关闭连接"""
        if self._engine:
            self._engine.dispose()
            self._engine = None


# 测试入口
if __name__ == "__main__":
    from hpf_platform.etl.config import get_oracle_connection_string
    
    inspector = OracleInspector(get_oracle_connection_string())
    
    if inspector.test_connection():
        print("✅ Oracle 连接成功")
        
        schema = "SHINEYUE40_BZBGJJYW_CS"
        tables = inspector.get_all_tables(schema)
        print(f"📋 找到 {len(tables)} 个表")
        
        if tables:
            # 获取第一个表的元数据
            meta = inspector.get_table_metadata(tables[0], schema)
            print(f"\n表: {meta.table_name}")
            print(f"  行数: {meta.row_count:,}")
            print(f"  大小: {meta.size_mb:.2f} MB")
            print(f"  分区: {meta.is_partitioned} ({meta.partition_count})")
            print(f"  主键: {meta.primary_key}")
            candidates_str = ', '.join([f"{c['name']}({c['non_null_pct']:.0f}%)" for c in meta.incremental_candidates]) if meta.incremental_candidates else '无'
            print(f"  增量候选: {candidates_str}")
    else:
        print("❌ Oracle 连接失败")
    
    inspector.close()
