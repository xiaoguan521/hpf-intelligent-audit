"""
Oracle 到 DuckDB 增量同步主程序
使用 dlt 实现增量数据加载
支持单线程和多线程并行模式
"""
import dlt
from sqlalchemy import create_engine
from typing import Iterator, Dict, Any, List, Generator
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue, Empty
import logging
import time
from pathlib import Path
from datetime import datetime
from decimal import Decimal
import oracledb
import sys
import os

# 将项目根目录添加到 sys.path，解决从子目录运行时的导入问题
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 配置日志
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# Oracle 初始化（使用智能配置）
# ============================================================================

from hpf_platform.etl.config import OracleConfig

# 智能初始化 Oracle 客户端（自动检测版本并选择模式）
oracle_mode = OracleConfig.init_oracle_client()
print(f"🔧 Oracle 模式: {oracle_mode.upper()}")
if OracleConfig.get_version() != "未知":
    print(f"📊 Oracle 版本: {OracleConfig.get_version()}")


# ============================================================================
# Oracle 到 PyArrow 类型映射
# ============================================================================

def oracle_type_to_pyarrow(oracle_type: str, precision: int = None, scale: int = None):
    """
    将 Oracle 数据类型映射到 PyArrow 类型
    
    Args:
        oracle_type: Oracle 数据类型名称
        precision: 精度（针对 NUMBER 类型）
        scale: 小数位数（针对 NUMBER 类型）
        
    Returns:
        PyArrow 数据类型
    """
    import pyarrow as pa
    
    oracle_type = oracle_type.upper()
    
    # 数字类型
    if oracle_type == 'NUMBER':
        if scale == 0 or scale is None:
            # 整数
            if precision is None or precision > 18:
                # 大整数用 decimal128(38, 0) 保存精度，避免 float64 溢出或精度丢失
                return pa.decimal128(38, 0)
            elif precision <= 4:
                return pa.int16()
            elif precision <= 9:
                return pa.int32()
            elif precision <= 18:
                return pa.int64()
        else:
            # 小数统一用 float64 (除非需要高精度金额，暂保留 float64 以兼容旧逻辑)
            return pa.float64()
    
    if oracle_type in ('INTEGER', 'INT', 'SMALLINT'):
        return pa.int64()
    
    if oracle_type in ('FLOAT', 'BINARY_FLOAT'):
        return pa.float32()
    
    if oracle_type in ('DOUBLE PRECISION', 'BINARY_DOUBLE'):
        return pa.float64()
    
    # 字符串类型
    if oracle_type in ('VARCHAR2', 'VARCHAR', 'NVARCHAR2', 'CHAR', 'NCHAR', 'CLOB', 'NCLOB', 'LONG'):
        return pa.string()
    
    # 日期时间类型
    if oracle_type == 'DATE':
        return pa.timestamp('s')  # 秒精度
    
    if oracle_type.startswith('TIMESTAMP'):
        return pa.timestamp('us')  # 微秒精度
    
    # 二进制类型
    if oracle_type in ('BLOB', 'RAW', 'LONG RAW'):
        return pa.binary()
    
    # 其他类型统一用 string
    return pa.string()


def get_table_schema_as_pyarrow(engine, table_name: str, schema: str = None):
    """
    从 Oracle 获取表结构并转换为 PyArrow Schema
    
    Args:
        engine: SQLAlchemy engine
        table_name: 表名
        schema: Oracle schema
        
    Returns:
        (pyarrow.Schema, List[str]) - PyArrow schema 和列名列表
    """
    import pyarrow as pa
    
    with engine.connect() as conn:
        raw_conn = conn.connection
        cursor = raw_conn.cursor()
        
        query = """
            SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE
            FROM ALL_TAB_COLUMNS 
            WHERE TABLE_NAME = UPPER(:tbl)
        """
        params = {'tbl': table_name}
        
        if schema:
            query += " AND OWNER = UPPER(:schema)"
            params['schema'] = schema
        
        query += " ORDER BY COLUMN_ID"
        
        cursor.execute(query, params)
        columns_info = cursor.fetchall()
        cursor.close()
        
        if not columns_info:
            return None, []
        
        fields = []
        column_names = []
        
        for col_name, data_type, precision, scale in columns_info:
            pa_type = oracle_type_to_pyarrow(data_type, precision, scale)
            # 所有字段都允许 null
            fields.append(pa.field(col_name, pa_type, nullable=True))
            column_names.append(col_name)
        
        return pa.schema(fields), column_names


# ============================================================================
# 并行读取器（用于多线程模式）
# ============================================================================

class OracleParallelReader:
    """Oracle 并行读取器 - 多线程分片读取"""
    
    def __init__(
        self,
        connection_string: str,
        table_name: str,
        schema: str = None,
        primary_key: str = "ID",
        num_workers: int = 4,
        batch_size: int = 50000
    ):
        self.connection_string = connection_string
        self.table_name = table_name
        self.schema = schema
        self.primary_key = primary_key
        self.num_workers = num_workers
        self.batch_size = batch_size
        self.full_table = f"{schema}.{table_name}" if schema else table_name
        self.stats = {'read': 0, 'start_time': None}
    
    def _get_engine(self):
        """创建数据库引擎"""
        return create_engine(
            self.connection_string,
            pool_size=self.num_workers + 2,
            max_overflow=self.num_workers,
            pool_pre_ping=True,
            pool_recycle=3600,
            echo=False
        )
    
    def get_columns(self, engine) -> List[str]:
        """获取表的列名"""
        with engine.connect() as conn:
            raw_conn = conn.connection
            cursor = raw_conn.cursor()
            
            query = "SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = UPPER(:tbl)"
            params = {'tbl': self.table_name}
            
            if self.schema:
                query += " AND OWNER = UPPER(:schema)"
                params['schema'] = self.schema
            
            query += " ORDER BY COLUMN_ID"
            cursor.execute(query, params)
            columns = [row[0] for row in cursor.fetchall()]
            cursor.close()
            
            return columns
    
    def get_id_range(self, engine, last_value: int = None) -> tuple:
        """获取 ID 范围"""
        with engine.connect() as conn:
            raw_conn = conn.connection
            cursor = raw_conn.cursor()
            
            if last_value:
                query = f"SELECT :last_val, MAX({self.primary_key}), COUNT(*) FROM {self.full_table} WHERE {self.primary_key} > :last_val"
                cursor.execute(query, {'last_val': last_value})
            else:
                query = f"SELECT MIN({self.primary_key}), MAX({self.primary_key}), COUNT(*) FROM {self.full_table}"
                cursor.execute(query)
            
            result = cursor.fetchone()
            cursor.close()
            
            min_id = (result[0] - 1) if result[0] else 0
            max_id = result[1] or 0
            count = result[2] or 0
            
            return min_id, max_id, count
    
    def calculate_chunks(self, min_id: int, max_id: int, total_count: int) -> List[tuple]:
        """
        一次查询获取所有分片边界点
        使用 ROW_NUMBER + MOD 筛选边界行
        """
        if total_count == 0:
            return [(0, min_id, max_id + 1)]
        
        num_chunks = min(self.num_workers * 2, total_count)
        
        if num_chunks <= 1:
            return [(0, min_id, max_id + 1)]
        
        rows_per_chunk = total_count // num_chunks
        
        engine = self._get_engine()
        try:
            with engine.connect() as conn:
                raw_conn = conn.connection
                cursor = raw_conn.cursor()
                
                # 直接在 SQL 中使用数值，避免绑定变量问题
                query = f"""
                    SELECT pk_val FROM (
                        SELECT {self.primary_key} as pk_val,
                               ROW_NUMBER() OVER (ORDER BY {self.primary_key}) as rn
                        FROM {self.full_table}
                    )
                    WHERE rn = 1 
                       OR MOD(rn, {rows_per_chunk}) = 0
                       OR rn = {total_count}
                    ORDER BY pk_val
                """
                cursor.execute(query)
                boundary_ids = [row[0] for row in cursor.fetchall()]
                cursor.close()
                
                # 调试输出：显示实际的边界ID
                print(f"  ├─ 边界ID: {[f'{x:,}' for x in boundary_ids[:5]]}...{[f'{x:,}' for x in boundary_ids[-2:]]}")
                
                if len(boundary_ids) < 2:
                    return [(0, min_id, max_id + 1)]
                
                # 构建分片：第一个分片从第一个边界ID-1开始（避免大量空洞扫描）
                chunks = []
                for i in range(len(boundary_ids) - 1):
                    # 所有分片都使用实际的边界ID
                    start = boundary_ids[i] - 1  # ID > start，所以减1确保包含边界值
                    end = boundary_ids[i + 1]
                    chunks.append((i, start, end))
                
                return chunks
        finally:
            engine.dispose()
    
    def read_chunk(self, engine, chunk: tuple, columns: List[str]) -> List[Dict]:
        """读取单个分片 - 使用独立连接避免并发问题"""
        chunk_id, start_id, end_id = chunk
        
        print(f"\n  [分片 {chunk_id}] 开始读取: ID {start_id:,} ~ {end_id:,}")
        chunk_start = time.time()
        
        from sqlalchemy import create_engine as ce
        local_engine = ce(
            self.connection_string,
            pool_size=1,
            max_overflow=0,
            pool_pre_ping=True,
            echo=False
        )
        
        try:
            with local_engine.connect() as conn:
                raw_conn = conn.connection
                cursor = raw_conn.cursor()
                cursor.arraysize = min(self.batch_size, 10000)
                cursor.prefetchrows = cursor.arraysize
                
                columns_str = ", ".join(columns)
                query = f"""
                    SELECT {columns_str} FROM {self.full_table}
                    WHERE {self.primary_key} > :start_id AND {self.primary_key} <= :end_id
                    ORDER BY {self.primary_key}
                """
                
                cursor.execute(query, {'start_id': start_id, 'end_id': end_id})
                rows = cursor.fetchall()
                cursor.close()
                
                chunk_elapsed = time.time() - chunk_start
                print(f"  [分片 {chunk_id}] 完成: {len(rows):,} 行, 耗时 {chunk_elapsed:.1f}秒")
                
                return [dict(zip(columns, row)) for row in rows]
        finally:
            local_engine.dispose()
    
    def get_partitions(self, engine) -> List[str]:
        """获取表的分区列表"""
        with engine.connect() as conn:
            raw_conn = conn.connection
            cursor = raw_conn.cursor()
            
            # 获取表的所有分区名
            table_owner = self.schema.upper() if self.schema else None
            table_name = self.table_name.upper()
            
            query = """
                SELECT PARTITION_NAME 
                FROM ALL_TAB_PARTITIONS 
                WHERE TABLE_NAME = :table_name
            """
            params = {'table_name': table_name}
            
            if table_owner:
                query += " AND TABLE_OWNER = :table_owner"
                params['table_owner'] = table_owner
            
            query += " ORDER BY PARTITION_POSITION"
            
            cursor.execute(query, params)
            partitions = [row[0] for row in cursor.fetchall()]
            cursor.close()
            
            return partitions
    
    def read_partition(self, partition_name: str, columns: List[str]) -> List[Dict]:
        """读取单个分区的所有数据（分批读取）"""
        print(f"\n  [分区 {partition_name}] 开始读取...")
        partition_start = time.time()
        
        from sqlalchemy import create_engine as ce
        local_engine = ce(
            self.connection_string,
            pool_size=1,
            max_overflow=0,
            pool_pre_ping=True,
            echo=False
        )
        
        all_rows = []
        try:
            with local_engine.connect() as conn:
                raw_conn = conn.connection
                cursor = raw_conn.cursor()
                cursor.arraysize = 10000
                cursor.prefetchrows = 10000
                
                columns_str = ", ".join(columns)
                query = f"""
                    SELECT {columns_str} 
                    FROM {self.full_table} PARTITION ({partition_name})
                    ORDER BY {self.primary_key}
                """
                
                cursor.execute(query)
                
                # 分批读取，避免一次性加载太多数据到内存
                batch_size = 50000
                batch_count = 0
                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break
                    
                    batch_count += 1
                    all_rows.extend([dict(zip(columns, row)) for row in rows])
                    
                    # 每隔一批打印进度
                    elapsed = time.time() - partition_start
                    print(f"  [分区 {partition_name}] 读取中: {len(all_rows):,} 行 | 批次 #{batch_count} | 耗时 {elapsed:.1f}秒", end='\r')
                
                cursor.close()
                
                partition_elapsed = time.time() - partition_start
                print(f"  [分区 {partition_name}] 完成: {len(all_rows):,} 行, 耗时 {partition_elapsed:.1f}秒    ")
                
                return all_rows
        finally:
            local_engine.dispose()
    
    def parallel_read_by_partition(self) -> Generator[Dict, None, None]:
        """按分区串行读取（边读边写，节省内存）"""
        self.stats['start_time'] = time.time()
        self.stats['read'] = 0
        
        engine = self._get_engine()
        
        try:
            columns = self.get_columns(engine)
            partitions = self.get_partitions(engine)
            
            print(f"  ├─ 表: {self.full_table}")
            print(f"  ├─ 列数: {len(columns)}")
            print(f"  ├─ 分区数: {len(partitions)}")
            print(f"  ├─ 读取模式: 按分区串行（边读边写）")
            
            if not partitions:
                print(f"  └─ 非分区表，退回到 ID 分片模式")
                yield from self.parallel_read(None)
                return
            
            print(f"  ├─ 分区列表: {partitions[:5]}...{partitions[-2:] if len(partitions) > 5 else ''}")
            
            # 串行处理每个分区，边读边写
            for idx, partition_name in enumerate(partitions):
                partition_start = time.time()
                partition_rows = 0
                
                # 直接使用 engine 读取，不创建新连接
                with engine.connect() as conn:
                    raw_conn = conn.connection
                    cursor = raw_conn.cursor()
                    cursor.arraysize = 10000
                    cursor.prefetchrows = 10000
                    
                    columns_str = ", ".join(columns)
                    query = f"""
                        SELECT {columns_str} 
                        FROM {self.full_table} PARTITION ({partition_name})
                        ORDER BY {self.primary_key}
                    """
                    
                    cursor.execute(query)
                    
                    # 分批读取并立即 yield
                    batch_count = 0
                    while True:
                        rows = cursor.fetchmany(50000)
                        if not rows:
                            break
                        
                        batch_count += 1
                        for row in rows:
                            yield dict(zip(columns, row))
                            self.stats['read'] += 1
                            partition_rows += 1
                        
                        # 实时进度
                        elapsed = time.time() - self.stats['start_time']
                        speed = self.stats['read'] / elapsed if elapsed > 0 else 0
                        print(f"  ├─ [{partition_name}] 读取中: {partition_rows:,} 行 | 总计: {self.stats['read']:,} | 速度: {speed:,.0f} 行/秒", end='\r')
                    
                    cursor.close()
                
                partition_elapsed = time.time() - partition_start
                print(f"  ├─ [{partition_name}] 完成: {partition_rows:,} 行, 耗时 {partition_elapsed:.1f}秒 ({idx+1}/{len(partitions)})    ")
            
            elapsed = time.time() - self.stats['start_time']
            speed = self.stats['read'] / elapsed if elapsed > 0 else 0
            print(f"\n  └─ 读取完成: {self.stats['read']:,} 行 | 耗时: {elapsed:.1f}秒 | 平均: {speed:,.0f} 行/秒")
        
        finally:
            engine.dispose()
    
    def parallel_read(self, last_value: int = None) -> Generator[Dict, None, None]:
        """并行读取所有数据"""
        self.stats['start_time'] = time.time()
        self.stats['read'] = 0
        
        engine = self._get_engine()
        
        try:
            columns = self.get_columns(engine)
            min_id, max_id, total_count = self.get_id_range(engine, last_value)
            
            print(f"  ├─ 表: {self.full_table}")
            print(f"  ├─ 列数: {len(columns)}")
            print(f"  ├─ ID 范围: {min_id:,} ~ {max_id:,}")
            print(f"  ├─ 预计行数: {total_count:,}")
            print(f"  ├─ 并行线程: {self.num_workers}")
            
            if total_count == 0:
                print(f"  └─ 无新数据")
                return
            
            print(f"  ├─ 正在计算分片边界（采样方式）...")
            chunks = self.calculate_chunks(min_id, max_id, total_count)
            print(f"  ├─ 分片数量: {len(chunks)}")
            
            # 队列存储结果，保证顺序
            result_queue = Queue()
            completed_chunks = {}
            next_chunk_to_yield = 0
            
            def reader_callback(future, chunk_id):
                try:
                    data = future.result()
                    result_queue.put((chunk_id, data))
                except Exception as e:
                    logger.error(f"分片 {chunk_id} 读取失败: {e}")
                    result_queue.put((chunk_id, []))
            
            with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
                futures = []
                for chunk in chunks:
                    future = executor.submit(self.read_chunk, engine, chunk, columns)
                    future.add_done_callback(lambda f, cid=chunk[0]: reader_callback(f, cid))
                    futures.append(future)
                
                chunks_received = 0
                while chunks_received < len(chunks):
                    try:
                        # 增加超时时间到600秒（10分钟），适应大范围分片
                        chunk_id, data = result_queue.get(timeout=600)
                        completed_chunks[chunk_id] = data
                        chunks_received += 1
                        
                        while next_chunk_to_yield in completed_chunks:
                            chunk_data = completed_chunks.pop(next_chunk_to_yield)
                            for row in chunk_data:
                                yield row
                                self.stats['read'] += 1
                            
                            next_chunk_to_yield += 1
                            
                            elapsed = time.time() - self.stats['start_time']
                            speed = self.stats['read'] / elapsed if elapsed > 0 else 0
                            print(f"  ├─ 读取: {self.stats['read']:,} 行 | 速度: {speed:,.0f} 行/秒 | 分片: {next_chunk_to_yield}/{len(chunks)}", end='\r')
                    
                    except Empty:
                        # 不是错误，只是还在等待
                        elapsed = time.time() - self.stats['start_time']
                        print(f"  ├─ 等待分片完成... 已等待 {elapsed:.0f} 秒", end='\r')
            
            elapsed = time.time() - self.stats['start_time']
            speed = self.stats['read'] / elapsed if elapsed > 0 else 0
            print(f"\n  └─ 读取完成: {self.stats['read']:,} 行 | 耗时: {elapsed:.1f}秒 | 平均: {speed:,.0f} 行/秒")
        
        finally:
            engine.dispose()


# ============================================================================
# dlt Resource 函数
# ============================================================================

def oracle_table_resource(
    connection_string: str,
    table_name: str,
    schema: str = None,
    incremental_column: str = None,
    batch_size: int = 50000,
    stats: dict = None,
    primary_key: str = "ID",
    parallel: bool = False,
    num_workers: int = 4
) -> Iterator[Dict[str, Any]]:
    """
    从 Oracle 表中读取数据的 dlt resource
    
    Args:
        connection_string: Oracle 数据库连接字符串
        table_name: 要同步的表名
        schema: 数据库 schema (可选)
        incremental_column: 用于增量加载的列名
        batch_size: 每批读取的行数
        stats: 统计信息字典
        primary_key: 主键列（用于分页）
        parallel: 是否使用多线程并行模式
        num_workers: 并行线程数（仅 parallel=True 时生效）
    """
    full_table_name = f"{schema}.{table_name}" if schema else table_name
    
    # 初始化统计信息
    if stats:
        stats[table_name] = {
            'rows': 0,
            'mode': '全量',
            'last_value': None
        }
    
    # 预先获取表的 PyArrow Schema（统一 Fast 和 Standard 模式）
    engine = create_engine(
        connection_string,
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
        echo=False
    )
    pyarrow_schema, _ = get_table_schema_as_pyarrow(engine, table_name, schema)
    engine.dispose()
    
    # 从 Schema 中提取 range_column 的类型
    range_column = incremental_column or primary_key
    range_col_type = None
    if pyarrow_schema:
        import pyarrow as pa
        for field in pyarrow_schema:
            if field.name == range_column:
                range_col_type = field.type
                break
    
    # 获取增量状态
    last_value = None
    if incremental_column:
        last_value = dlt.current.resource_state().get('last_value')
        if last_value:
            if stats:
                stats[table_name]['mode'] = '增量'
                stats[table_name]['last_value'] = str(last_value)
            print(f"  ├─ 增量模式: 从 {incremental_column} > {last_value} 开始")
    
    # 选择读取模式
    if parallel:
        # 多线程并行模式
        print(f"  ├─ 读取模式: 多线程并行 ({num_workers} workers)")
        yield from _parallel_read(
            connection_string, table_name, schema, primary_key,
            num_workers, batch_size, incremental_column, last_value, stats
        )
    else:
        # 单线程顺序模式
        print(f"  ├─ 读取模式: 单线程顺序")
        yield from _sequential_read(
            connection_string, table_name, schema, primary_key,
            batch_size, incremental_column, last_value, stats, range_col_type
        )


def _parallel_read(
    connection_string: str,
    table_name: str,
    schema: str,
    primary_key: str,
    num_workers: int,
    batch_size: int,
    incremental_column: str,
    last_value: int,
    stats: dict
) -> Generator[Dict, None, None]:
    """多线程并行读取"""
    reader = OracleParallelReader(
        connection_string=connection_string,
        table_name=table_name,
        schema=schema,
        primary_key=primary_key,
        num_workers=num_workers,
        batch_size=batch_size
    )
    
    max_value = last_value
    row_count = 0
    
    for row in reader.parallel_read(last_value):
        if incremental_column and incremental_column in row:
            row_value = row[incremental_column]
            if max_value is None or row_value > max_value:
                max_value = row_value
        
        yield row
        row_count += 1
    
    # 保存增量状态
    if incremental_column and max_value is not None:
        dlt.current.resource_state()['last_value'] = max_value
    
    if stats:
        stats[table_name]['rows'] = row_count


def _sequential_read(
    connection_string: str,
    table_name: str,
    schema: str,
    primary_key: str,
    batch_size: int,
    incremental_column: str,
    last_value: int,
    stats: dict,
    range_col_type = None  # PyArrow 类型
) -> Generator[Dict, None, None]:
    """单线程顺序读取（使用 ID 范围分页）"""
    print(f"DEBUG: _sequential_read called with table_name='{table_name}', schema='{schema}'")
    # 如果表名已经包含 . (例如 SCHEMA.TABLE)，则不再拼接 schema
    if "." in table_name:
        full_table_name = table_name
        # 尝试从 table_name 中提取真实表名用于元数据查询
        real_table_name = table_name.split(".")[-1]
    else:
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        real_table_name = table_name
    range_column = incremental_column or primary_key
    
    engine = create_engine(
        connection_string,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        pool_recycle=3600,
        echo=False
    )
    
    try:
        with engine.connect() as conn:
            raw_conn = conn.connection
            
            # 获取列名
            columns_query = f"""
                SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS 
                WHERE TABLE_NAME = UPPER(:tbl) 
                {"AND OWNER = UPPER(:schema)" if schema else ""}
                ORDER BY COLUMN_ID
            """
            cursor = raw_conn.cursor()
            if schema:
                cursor.execute(columns_query, {'tbl': real_table_name, 'schema': schema})
            else:
                cursor.execute(columns_query, {'tbl': real_table_name})
            columns = [row[0] for row in cursor.fetchall()]
            cursor.close()
            
            if not columns:
                print(f"  ⚠️ 无法获取表 {real_table_name} 的列信息，回退到 SELECT *")
                columns_str = "*"
            else:
                columns_str = ", ".join(columns)
            
            # 获取起始值（使用传入的 PyArrow 类型判断）
            if last_value is None:
                import pyarrow as pa
                # 判断是否为 DATE 类型
                is_date_type = range_col_type and pa.types.is_timestamp(range_col_type)
                
                if is_date_type:
                    last_value = "DATE '1900-01-01'"  # SQL 字面量
                else:
                    min_query = f"SELECT MIN({range_column}) FROM {full_table_name}"
                    cursor = raw_conn.cursor()
                    cursor.execute(min_query)
                    min_result = cursor.fetchone()
                    cursor.close()
                    last_value = (min_result[0] - 1) if min_result and min_result[0] else 0
            
            print(f"  ├─ 表: {full_table_name}")
            print(f"  ├─ 分页列: {range_column}, 起始值: {last_value}")
            print(f"  ├─ 批量大小: {batch_size:,}")
            
            # 保存初始 last_value，用于判断是否为增量恢复
            initial_value = last_value
            
            row_count = 0
            batch_num = 0
            start_time = time.time()
            
            # 判断是否为真正的增量恢复（从 dlt state 恢复的值）
            # 如果是第一次同步（last_value 刚从函数参数传入的 None 计算而来），使用 >= 包含边界
            is_first_sync = (last_value == initial_value)
            
            
            while True:
                batch_num += 1
                
                # 如果 last_value 是 SQL 字面量（DATE），直接拼接而不是用绑定变量
                if isinstance(last_value, str) and last_value.startswith("DATE "):
                    # 对于首次同步，使用 >= 以包含边界值和所有非 NULL 数据
                    operator = ">=" if is_first_sync else ">"
                    range_query = f"""
                        SELECT {columns_str} FROM (
                            SELECT {columns_str} FROM {full_table_name}
                            WHERE {range_column} {operator} {last_value}
                            ORDER BY {range_column}
                        ) WHERE ROWNUM <= :batch_size
                    """
                    bind_vars = {'batch_size': batch_size}
                else:
                    # 对于首次同步，使用 >= 以包含边界值
                    operator = ">=" if is_first_sync else ">"
                    range_query = f"""
                        SELECT {columns_str} FROM (
                            SELECT {columns_str} FROM {full_table_name}
                            WHERE {range_column} {operator} :last_val
                            ORDER BY {range_column}
                        ) WHERE ROWNUM <= :batch_size
                    """
                    bind_vars = {'last_val': last_value, 'batch_size': batch_size}
                
                
                cursor = raw_conn.cursor()
                cursor.arraysize = min(batch_size, 10000)
                cursor.prefetchrows = cursor.arraysize
                
                cursor.execute(range_query, bind_vars)
                
                
                col_names = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                cursor.close()
                
                if not rows:
                    break
                
                range_col_idx = col_names.index(range_column) if range_column in col_names else 0
                last_value = rows[-1][range_col_idx]
                
                batch_data = [dict(zip(col_names, row)) for row in rows]
                
                if incremental_column:
                    dlt.current.resource_state()['last_value'] = last_value
                
                yield from batch_data
                
                row_count += len(batch_data)
                
                if stats:
                    stats[table_name]['rows'] = row_count
                    elapsed = time.time() - start_time
                    speed = row_count / elapsed if elapsed > 0 else 0
                    print(f"  ├─ 进度: {row_count:,} 行 | 速度: {speed:,.0f} 行/秒 | 批次 #{batch_num}", end='\r')
                
                if len(rows) < batch_size:
                    break
            
            elapsed = time.time() - start_time
            speed = row_count / elapsed if elapsed > 0 else 0
            if stats:
                stats[table_name]['rows'] = row_count
            print(f"\n  └─ 完成: {row_count:,} 行 | 总耗时: {elapsed:.1f}秒 | 平均: {speed:,.0f} 行/秒")
    
    except Exception as e:
        # 优化错误输出，避免满屏 Traceback，除非是调试模式
        error_msg = str(e)
        if "ORA-" in error_msg:
            print(f"\n  ❌ Oracle 错误: {error_msg.split('Help:')[0].strip()}")
            if "ORA-00933" in error_msg:
                print(f"     提示: SQL 语法错误，当前查询表名: {full_table_name}")
        else:
            print(f"\n  ❌ 同步出错: {error_msg}")
            import traceback
            # traceback.print_exc() # 用户要求减少混乱输出
        
        raise Exception(error_msg)
    
    finally:
        engine.dispose()


# ============================================================================
# dlt Source 函数
# ============================================================================

@dlt.source
def oracle_source(
    connection_string: str,
    tables: list[dict],
    stats: dict = None,
    parallel: bool = False,
    num_workers: int = 4
):
    """
    Oracle 数据源
    
    Args:
        connection_string: Oracle 连接字符串
        tables: 要同步的表配置列表
        stats: 统计信息字典
        parallel: 是否使用多线程并行模式
        num_workers: 并行线程数
    """
    for table_config in tables:
        table_name = table_config["name"]
        incremental_column = table_config.get("incremental_column")
        schema = table_config.get("schema")
        batch_size = table_config.get("batch_size", 50000)
        primary_key = table_config.get("primary_key", "ID")
        
        # 初始化统计信息
        if stats is not None:
            stats[table_name] = {
                'rows': 0,
                'mode': '未知',
                'last_value': None
            }
        
        # 1. 获取表结构并转换为 dlt columns
        dlt_columns = {}
        try:
            # 创建临时引擎获取元数据
            engine = create_engine(
                connection_string,
                pool_size=1, max_overflow=0,
                pool_pre_ping=True, echo=False
            )
            pa_schema, _ = get_table_schema_as_pyarrow(engine, table_name, schema)
            engine.dispose()
            
            if pa_schema:
                import pyarrow as pa
                for field in pa_schema:
                    dlt_type = "text"
                    t = field.type
                    if pa.types.is_string(t): dlt_type = "text"
                    elif pa.types.is_integer(t): dlt_type = "bigint"
                    elif pa.types.is_floating(t): dlt_type = "double"
                    elif pa.types.is_decimal(t): dlt_type = "decimal"
                    elif pa.types.is_timestamp(t): dlt_type = "timestamp"
                    elif pa.types.is_date(t): dlt_type = "date"
                    elif pa.types.is_binary(t): dlt_type = "binary"
                    elif pa.types.is_boolean(t): dlt_type = "bool"
                    dlt_columns[field.name] = {"name": field.name, "data_type": dlt_type, "nullable": field.nullable}
        except Exception as e:
            print(f"  ⚠️ Schema 预获取失败: {e}")
        
        yield dlt.resource(
            oracle_table_resource(
                connection_string=connection_string,
                table_name=table_name,
                schema=schema,
                incremental_column=incremental_column,
                batch_size=batch_size,
                stats=stats,
                primary_key=primary_key,
                parallel=parallel,
                num_workers=num_workers
            ),
            name=table_name,
            write_disposition="merge" if incremental_column else "replace",
            primary_key=primary_key,
            columns=dlt_columns  # 显式传入列定义
        )


# ============================================================================
# 工具函数
# ============================================================================

def format_size(size_bytes: int) -> str:
    """格式化文件大小"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"


def format_duration(seconds: float) -> str:
    """格式化时间"""
    if seconds < 60:
        return f"{seconds:.2f} 秒"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.2f} 分钟"
    else:
        hours = seconds / 3600
        return f"{hours:.2f} 小时"


# ============================================================================
# 主函数
# ============================================================================

def run_sync(
    oracle_conn: str,
    tables: list[dict],
    duckdb_path: str = "oracle_sync.duckdb",
    parallel: bool = False,
    num_workers: int = 4
):
    """
    执行同步任务
    
    Args:
        oracle_conn: Oracle 连接字符串
        tables: 表配置列表
        duckdb_path: DuckDB 数据库文件路径
        parallel: 是否使用多线程并行模式
        num_workers: 并行线程数（仅 parallel=True 时生效）
    """
    print("\n" + "=" * 70)
    print("🚀 Oracle → DuckDB 增量同步任务")
    print("=" * 70)
    print(f"📅 开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📊 目标数据库: {duckdb_path}")
    print(f"📋 同步表数量: {len(tables)}")
    if parallel:
        print(f"🔧 读取模式: 多线程并行 ({num_workers} workers)")
    else:
        print(f"🔧 读取模式: 单线程顺序")
    if oracle_mode == "thick":
        print("🔧 Oracle 模式: Thick (兼容所有版本)")
        client_dir = os.getenv("ORACLE_CLIENT_DIR")
        if client_dir:
            print(f"📁 Instant Client: {Path(client_dir).name}")
    else:
        print("🔧 Oracle 模式: Thin (仅支持 12.1+)")
    print()
    
    # 记录文件初始大小
    db_file = Path(duckdb_path)
    initial_size = db_file.stat().st_size if db_file.exists() else 0
    
    # 统计信息
    stats = {}
    
    # 显示表配置
    print("📌 同步配置:")
    for i, table_config in enumerate(tables, 1):
        table_name = table_config["name"]
        inc_col = table_config.get("incremental_column", "无")
        pk = table_config.get("primary_key", "无")
        print(f"  {i}. {table_name}")
        print(f"     ├─ 增量列: {inc_col}")
        print(f"     └─ 主键: {pk}")
    print()
    
    # 开始同步
    print("⏳ 开始数据同步...")
    start_time = time.time()
    
    try:
        # 创建 pipeline（优化配置）
        pipeline = dlt.pipeline(
            pipeline_name="oracle_to_duckdb",
            destination=dlt.destinations.duckdb(duckdb_path),
            dataset_name="oracle_data",
            progress="log"  # 显示写入进度
        )
        
        # 加载数据
        source = oracle_source(
            oracle_conn, 
            tables, 
            stats,
            parallel=parallel,
            num_workers=num_workers
        )
        
        print("⏳ 正在写入 DuckDB（大数据量可能需要较长时间）...")
        load_info = pipeline.run(source)
        
        # 计算耗时
        end_time = time.time()
        duration = end_time - start_time
        
        # 计算文件大小变化
        final_size = db_file.stat().st_size if db_file.exists() else 0
        size_increase = final_size - initial_size
        
        # 计算总行数
        total_rows = sum(s['rows'] for s in stats.values())
        
        # 显示结果
        print()
        print("=" * 70)
        print("✅ 同步完成!")
        print("=" * 70)
        print()
        
        print("📊 同步统计:")
        print(f"  ├─ 总耗时: {format_duration(duration)}")
        print(f"  ├─ 总行数: {total_rows:,} 行")
        if duration > 0:
            throughput = total_rows / duration
            print(f"  ├─ 吞吐量: {throughput:,.0f} 行/秒")
        print(f"  ├─ 数据库大小: {format_size(final_size)}")
        if size_increase > 0:
            print(f"  └─ 新增数据: {format_size(size_increase)}")
        print()
        
        print("📋 各表详情:")
        for i, (table_name, table_stats) in enumerate(stats.items(), 1):
            mode = table_stats['mode']
            rows = table_stats['rows']
            last_val = table_stats.get('last_value')
            
            print(f"  {i}. {table_name}")
            print(f"     ├─ 模式: {mode}")
            print(f"     ├─ 行数: {rows:,}")
            if last_val:
                print(f"     └─ 最新值: {last_val}")
            else:
                print(f"     └─ 最新值: -")
        
        print()
        print("=" * 70)
        print(f"🎉 任务完成于: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)
        print()
        
        return load_info
        
    except Exception as e:
        end_time = time.time()
        duration = end_time - start_time
        
        print()
        print("=" * 70)
        print("❌ 同步失败!")
        print("=" * 70)
        print(f"⏱️  耗时: {format_duration(duration)}")
        print(f"❗ 错误: {str(e)}")
        print("=" * 70)
        print()
        import traceback
        traceback.print_exc()
        raise


# ============================================================================
# 高速同步模式（PyArrow 直接写入 + dlt 状态关联）
# ============================================================================

def _update_dlt_state(pipeline_name: str, table_name: str, last_value: Any):
    """更新 dlt 的增量状态文件"""
    import json
    
    # dlt 状态文件路径
    # Windows: C:/Users/<user>/.dlt/pipelines/<pipeline_name>/state/
    # Linux/Mac: ~/.dlt/pipelines/<pipeline_name>/state/
    home = Path.home()
    state_dir = home / ".dlt" / "pipelines" / pipeline_name / "state"
    state_file = state_dir / "state.json"
    
    state_dir.mkdir(parents=True, exist_ok=True)
    
    if state_file.exists():
        with open(state_file, 'r') as f:
            state = json.load(f)
    else:
        state = {
            "pipeline_name": pipeline_name,
            "first_run": True,
            "sources": {}
        }
    
    source_key = "oracle_source"
    if "sources" not in state:
        state["sources"] = {}
    if source_key not in state["sources"]:
        state["sources"][source_key] = {"resources": {}}
    if "resources" not in state["sources"][source_key]:
        state["sources"][source_key]["resources"] = {}
    
    state["sources"][source_key]["resources"][table_name] = {
        "last_value": last_value
    }
    state["first_run"] = False
    
    with open(state_file, 'w') as f:
        json.dump(state, f, indent=2, default=str)
    
    logger.info(f"dlt 状态已更新: {state_file}")


def _sync_partition_worker(
    oracle_conn: str,
    temp_duckdb_path: str,  # 改为独立的临时 DuckDB 文件路径
    schema: str,
    table_name: str,
    partition_name: str,
    primary_key: str,
    batch_size: int = 50000,
    pyarrow_schema = None  # 添加 schema 参数
) -> dict:
    """
    单个分区同步工作函数（在独立线程中运行）
    读取 Oracle 分区数据，写入独立的 DuckDB 临时文件
    """
    import duckdb
    import pyarrow as pa
    from sqlalchemy import create_engine
    from decimal import Decimal
    
    start_time = time.time()
    row_count = 0
    max_value = None
    
    full_table = f"{schema}.{table_name}" if schema else table_name
    
    print(f"\n  [分区 {partition_name}] 开始读取...")
    
    # 创建独立的 Oracle 连接
    engine = create_engine(
        oracle_conn,
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
        echo=False
    )
    
    # 每个线程创建独立的 DuckDB 临时文件
    duck_conn = duckdb.connect(temp_duckdb_path)
    
    try:
        with engine.connect() as conn:
            raw_conn = conn.connection
            cursor = raw_conn.cursor()
            cursor.arraysize = 10000
            cursor.prefetchrows = 10000
            
            # 获取列名
            cursor.execute(f"SELECT * FROM {full_table} WHERE ROWNUM = 0")
            columns = [desc[0] for desc in cursor.description]
            cursor.close()
            
            # 获取分区总行数（用于计算进度）
            cursor = raw_conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {full_table} PARTITION ({partition_name})")
            total_rows_in_partition = cursor.fetchone()[0]
            cursor.close()
            
            if total_rows_in_partition == 0:
                print(f"  [分区 {partition_name}] 完成: 0 行, 耗时 0.0秒    ")
                return {
                    'partition': partition_name,
                    'temp_file': temp_duckdb_path,
                    'rows': 0,
                    'max_value': None,
                    'duration': 0,
                    'success': True
                }
            
            print(f"  [分区 {partition_name}] 总行数: {total_rows_in_partition:,}")
            
            # 读取分区数据
            cursor = raw_conn.cursor()
            cursor.arraysize = 10000
            cursor.prefetchrows = 10000
            
            columns_str = ", ".join(columns)
            query = f"""
                SELECT {columns_str} 
                FROM {full_table} PARTITION ({partition_name})
                ORDER BY {primary_key}
            """
            
            cursor.execute(query)
            
            first_batch = True
            batch_count = 0
            batch_rows = []
            
            # 预处理 Schema 类型，优化转换性能
            float_cols = set()
            int_cols = set()
            if pyarrow_schema:
                for field in pyarrow_schema:
                    if pa.types.is_floating(field.type):
                        float_cols.add(field.name)
                    elif pa.types.is_integer(field.type):
                        int_cols.add(field.name)
            
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                
                batch_count += 1
                for row in rows:
                    # 将行数据转换为字典
                    row_dict = dict(zip(columns, row))
                    
                    # 智能类型转换：根据 Schema 决定如何处理 Decimal
                    for col, val in row_dict.items():
                        if isinstance(val, (Decimal,)):
                            if val is None:
                                continue
                            
                            if col in float_cols:
                                # 目标是 float，强制转 float
                                row_dict[col] = float(val)
                            elif col in int_cols:
                                # 目标是 int，强制转 int
                                row_dict[col] = int(val)
                            # else: 目标是 Decimal/String，保持原样让 PyArrow 处理
                                
                    batch_rows.append(row_dict)
                    row_count += 1
                    
                    # 追踪最大值
                    if primary_key in row_dict:
                        val = row_dict[primary_key]
                        if max_value is None or val > max_value:
                            max_value = val
                
                # 每 batch_size 行写入一次
                if len(batch_rows) >= batch_size:
                    arrow_table = pa.Table.from_pylist(batch_rows, schema=pyarrow_schema)
                    
                    if first_batch:
                        duck_conn.execute("DROP TABLE IF EXISTS data")
                        duck_conn.execute("CREATE TABLE data AS SELECT * FROM arrow_table")
                        first_batch = False
                    else:
                        duck_conn.execute("INSERT INTO data SELECT * FROM arrow_table")
                    
                    batch_rows = []
                    
                    elapsed = time.time() - start_time
                    speed = row_count / elapsed if elapsed > 0 else 0
                    # 计算进度和预计剩余时间
                    progress = (row_count / total_rows_in_partition * 100) if total_rows_in_partition > 0 else 0
                    if speed > 0:
                        remaining_rows = total_rows_in_partition - row_count
                        eta_seconds = remaining_rows / speed
                        eta_str = f"剩余: {eta_seconds:.0f}秒"
                    else:
                        eta_str = "计算中..."
                    print(f"  [分区 {partition_name}] {progress:.1f}% | {row_count:,}/{total_rows_in_partition:,} | {speed:,.0f}行/秒 | {eta_str}", end='\r')
            
            # 处理剩余数据
            if batch_rows:
                arrow_table = pa.Table.from_pylist(batch_rows, schema=pyarrow_schema)
                if first_batch:
                    duck_conn.execute("DROP TABLE IF EXISTS data")
                    duck_conn.execute("CREATE TABLE data AS SELECT * FROM arrow_table")
                else:
                    duck_conn.execute("INSERT INTO data SELECT * FROM arrow_table")
            
            cursor.close()
        
        elapsed = time.time() - start_time
        print(f"  [分区 {partition_name}] 完成: {row_count:,} 行, 耗时 {elapsed:.1f}秒    ")
        
        return {
            'partition': partition_name,
            'temp_file': temp_duckdb_path,  # 返回临时文件路径
            'rows': row_count,
            'max_value': max_value,
            'duration': elapsed,
            'success': True
        }
        
    except Exception as e:
        logger.error(f"分区 {partition_name} 同步失败: {e}")
        return {
            'partition': partition_name,
            'temp_file': temp_duckdb_path,
            'rows': 0,
            'max_value': None,
            'duration': 0,
            'success': False,
            'error': str(e)
        }
    finally:
        duck_conn.close()
        engine.dispose()

def run_fast_sync(
    oracle_conn: str,
    tables: list[dict],
    duckdb_path: str = "oracle_sync.duckdb",
    num_workers: int = 4,
    pipeline_name: str = "oracle_to_duckdb",
    dataset_name: str = "oracle_data",
    use_partition: bool = False
):
    """
    高速同步：使用 PyArrow 直接写入 DuckDB，然后更新 dlt 状态
    
    适用于：首次全量同步（千万级数据）
    use_partition=True 时使用按分区并行读取（推荐用于分区表）
    后续增量同步仍可使用 run_sync()
    """
    import duckdb
    import pyarrow as pa
    
    print("\n" + "=" * 70)
    print("🚀 Oracle → DuckDB 高速同步（PyArrow 直接写入）")
    print("=" * 70)
    print(f"📅 开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📊 目标数据库: {duckdb_path}")
    print(f"📋 同步表数量: {len(tables)}")
    print(f"🔧 并行线程: {num_workers}")
    print(f"📂 读取模式: {'按分区并行' if use_partition else 'ID 分片并行'}")
    print()
    
    db_file = Path(duckdb_path)
    initial_size = db_file.stat().st_size if db_file.exists() else 0
    start_time = time.time()
    
    duck_conn = duckdb.connect(duckdb_path)
    duck_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {dataset_name}")
    
    results = {}
    
    for table_config in tables:
        table_name = table_config["name"]
        schema = table_config.get("schema")
        primary_key = table_config.get("primary_key", "ID")
        incremental_column = table_config.get("incremental_column")
        batch_size = table_config.get("batch_size", 50000)
        
        print(f"\n📊 同步表: {table_name}")
        print("-" * 50)
        
        full_table_name = f"{dataset_name}.{table_name}"
        table_start = time.time()
        
        # 分区模式：并行读取各分区到临时表，最后合并
        if use_partition:
            # 先删除目标表
            duck_conn.execute(f"DROP TABLE IF EXISTS {full_table_name}")
            
            # 获取分区列表
            reader = OracleParallelReader(
                connection_string=oracle_conn,
                table_name=table_name,
                schema=schema,
                primary_key=primary_key,
                num_workers=num_workers,
                batch_size=batch_size
            )
            
            engine = reader._get_engine()
            
            # 获取统一的 PyArrow schema
            print(f"  🔍 获取表结构并生成统一 Schema...")
            pyarrow_schema, _ = get_table_schema_as_pyarrow(
                engine, 
                table_name, 
                schema
            )
            print(f"  ✅ Schema 已生成: {[f.name for f in pyarrow_schema]}")
            
            partitions = reader.get_partitions(engine)
            engine.dispose()
            
            print(f"  ├─ 分区数: {len(partitions)}")
            print(f"  ├─ 并行线程: {num_workers}")
            print(f"  ├─ 读取模式: 并行分区写入独立临时文件")
            print(f"  ├─ 分区列表: {partitions[:3]}...{partitions[-2:] if len(partitions) > 5 else ''}")
            print("⏳ 开始并行同步...")
            
            # 并行处理分区
            partition_results = []
            
            # 获取 DuckDB 文件所在目录
            import os
            duckdb_dir = os.path.dirname(os.path.abspath(duckdb_path))
            
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                futures = []
                for idx, partition in enumerate(partitions):
                    # 每个分区使用独立的临时 DuckDB 文件
                    temp_file = os.path.join(duckdb_dir, f"temp_partition_{idx}.duckdb")
                    
                    future = executor.submit(
                        _sync_partition_worker,
                        oracle_conn,
                        temp_file,  # 独立的临时文件
                        schema,
                        table_name,
                        partition,
                        primary_key,
                        batch_size,
                        pyarrow_schema  # 传递统一 schema
                    )
                    futures.append(future)
                
                # 等待所有分区完成
                for future in futures:
                    result = future.result()
                    partition_results.append(result)
            
            # 统计结果
            total_rows = sum(r['rows'] for r in partition_results if r['success'])
            max_value = None
            for r in partition_results:
                if r['success'] and r['max_value'] is not None:
                    if max_value is None or r['max_value'] > max_value:
                        max_value = r['max_value']
            
            # 只收集成功且有数据的临时文件
            valid_temp_files = []
            for r in partition_results:
                if r['success'] and r['rows'] > 0:
                    valid_temp_files.append(r['temp_file'])
            
            print(f"\n  📦 合并 {len(valid_temp_files)} 个有效临时文件...")
            
            if valid_temp_files:
                # 先 ATTACH 所有临时文件
                for idx, temp_file in enumerate(valid_temp_files):
                    db_alias = f"temp_db_{idx}"
                    try:
                        duck_conn.execute(f"ATTACH '{temp_file}' AS {db_alias} (READ_ONLY)")
                    except Exception as e:
                        logger.warning(f"ATTACH 临时文件 {temp_file} 失败: {e}")
                
                # 使用 UNION ALL BY NAME 一次性合并（自动处理类型差异）
                union_parts = [f"SELECT * FROM temp_db_{idx}.data" for idx in range(len(valid_temp_files))]
                union_query = " UNION ALL BY NAME ".join(union_parts)
                
                try:
                    duck_conn.execute(f"CREATE TABLE {full_table_name} AS {union_query}")
                    print(f"  ├─ 合并完成")
                except Exception as e:
                    logger.error(f"合并失败: {e}")
                    # 回退到逐个合并
                    print(f"  ├─ UNION ALL 失败，尝试逐个合并...")
                    first_file = True
                    for idx in range(len(valid_temp_files)):
                        try:
                            if first_file:
                                duck_conn.execute(f"CREATE TABLE {full_table_name} AS SELECT * FROM temp_db_{idx}.data")
                                first_file = False
                            else:
                                # 使用 INSERT OR IGNORE 忽略类型错误
                                duck_conn.execute(f"INSERT INTO {full_table_name} SELECT * FROM temp_db_{idx}.data")
                        except Exception as ex:
                            logger.warning(f"合并 temp_db_{idx} 失败: {ex}")
                
                # DETACH 所有临时文件
                for idx in range(len(valid_temp_files)):
                    try:
                        duck_conn.execute(f"DETACH temp_db_{idx}")
                    except:
                        pass
            else:
                print(f"  ├─ 没有有效的临时文件")
                row_count = 0
            
            # 删除临时文件
            for r in partition_results:
                try:
                    temp_file = r.get('temp_file')
                    if temp_file and os.path.exists(temp_file):
                        os.remove(temp_file)
                        # 同时删除 WAL 文件
                        wal_file = temp_file + ".wal"
                        if os.path.exists(wal_file):
                            os.remove(wal_file)
                except Exception:
                    pass
            print(f"  ├─ 临时文件已清理")
            
            row_count = total_rows
            
        else:
            # 非分区模式：使用原有的 ID 分片并行读取
            reader = OracleParallelReader(
                connection_string=oracle_conn,
                table_name=table_name,
                schema=schema,
                primary_key=primary_key,
                num_workers=num_workers,
                batch_size=batch_size
            )
            
            # 获取统一的 PyArrow schema
            engine = reader._get_engine()
            print(f"  🔍 获取表结构并生成统一 Schema...")
            pyarrow_schema, _ = get_table_schema_as_pyarrow(engine, table_name, schema)
            engine.dispose()
            print(f"  ✅ Schema 已生成")
            
            row_count = 0
            max_value = None
            batch_rows = []
            batch_num = 0
            first_batch = True
            
            duck_conn.execute(f"DROP TABLE IF EXISTS {full_table_name}")
            
            print("⏳ 开始读取和写入...")
            
            # 预处理 Schema 类型，优化转换性能
            float_cols = set()
            int_cols = set()
            if pyarrow_schema:
                for field in pyarrow_schema:
                    if pa.types.is_floating(field.type):
                        float_cols.add(field.name)
                    elif pa.types.is_integer(field.type):
                        int_cols.add(field.name)
            
            for row in reader.parallel_read(None):
                batch_rows.append(row)
                
                if incremental_column and incremental_column in row:
                    val = row[incremental_column]
                    if max_value is None or val > max_value:
                        max_value = val
                
                # 智能类型转换：根据 Schema 决定如何处理 Decimal
                for col, val in row.items():
                    if isinstance(val, (Decimal,)):
                        if val is None:
                            continue
                        
                        if col in float_cols:
                            row[col] = float(val)
                        elif col in int_cols:
                            row[col] = int(val)
                        # else: 目标是 Decimal/String，保持原样让 PyArrow 处理
                
                if len(batch_rows) >= batch_size:
                    batch_num += 1
                    arrow_table = pa.Table.from_pylist(batch_rows, schema=pyarrow_schema)
                    
                    if first_batch:
                        duck_conn.execute(f"CREATE TABLE {full_table_name} AS SELECT * FROM arrow_table")
                        first_batch = False
                    else:
                        duck_conn.execute(f"INSERT INTO {full_table_name} SELECT * FROM arrow_table")
                    
                    row_count += len(batch_rows)
                    elapsed = time.time() - table_start
                    speed = row_count / elapsed if elapsed > 0 else 0
                    print(f"  ├─ 已写入: {row_count:,} 行 | 速度: {speed:,.0f} 行/秒 | 批次 #{batch_num}", end='\r')
                    
                    batch_rows = []
            
            if batch_rows:
                arrow_table = pa.Table.from_pylist(batch_rows, schema=pyarrow_schema)
                if first_batch:
                    duck_conn.execute(f"CREATE TABLE {full_table_name} AS SELECT * FROM arrow_table")
                else:
                    duck_conn.execute(f"INSERT INTO {full_table_name} SELECT * FROM arrow_table")
                row_count += len(batch_rows)
        
        table_elapsed = time.time() - table_start
        table_speed = row_count / table_elapsed if table_elapsed > 0 else 0
        
        print(f"\n  └─ 表 {table_name} 完成: {row_count:,} 行 | 耗时: {table_elapsed:.1f}秒 | 速度: {table_speed:,.0f} 行/秒")
        
        results[table_name] = {
            'rows': row_count,
            'max_value': max_value,
            'duration': table_elapsed
        }
        
        if incremental_column and max_value is not None:
            _update_dlt_state(
                pipeline_name=pipeline_name,
                table_name=table_name,
                last_value=max_value
            )
            print(f"  ✅ dlt 状态已更新: {incremental_column} = {max_value}")
    
    duck_conn.close()
    
    end_time = time.time()
    duration = end_time - start_time
    total_rows = sum(r['rows'] for r in results.values())
    
    final_size = db_file.stat().st_size if db_file.exists() else 0
    size_increase = final_size - initial_size
    
    print()
    print("=" * 70)
    print("✅ 高速同步完成!")
    print("=" * 70)
    print(f"  ├─ 总耗时: {format_duration(duration)}")
    print(f"  ├─ 总行数: {total_rows:,} 行")
    if duration > 0:
        print(f"  ├─ 平均速度: {total_rows/duration:,.0f} 行/秒")
    print(f"  ├─ 数据库大小: {format_size(final_size)}")
    if size_increase > 0:
        print(f"  └─ 新增数据: {format_size(size_increase)}")
    print()
    print("💡 后续增量同步请使用: run_sync(...)")
    print("=" * 70)
    
    return results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Oracle → DuckDB 数据同步工具")
    parser.add_argument("--fast", action="store_true", 
                        help="使用高速模式（PyArrow 直接写入），适合首次全量同步")
    parser.add_argument("--partition", action="store_true",
                        help="按分区并行读取（推荐用于分区表，如 IM_ZJ_LS）")
    parser.add_argument("--workers", type=int, default=4,
                        help="并行线程数（默认: 4）")
    parser.add_argument("--batch-size", type=int, default=50000,
                        help="每批读取行数（默认: 50000）")
    parser.add_argument("--db", type=str, default="oracle_sync.duckdb",
                        help="DuckDB 数据库路径（默认: oracle_sync.duckdb）")
    
    # 智能同步参数
    parser.add_argument("--smart", action="store_true",
                        help="启用 LLM 智能同步模式（自动分析表并推荐策略）")
    parser.add_argument("--auto", action="store_true",
                        help="全自动模式：跳过审批确认（仅 --smart 模式有效）")
    parser.add_argument("--schema", type=str, default=None,
                        help="Oracle schema（默认从环境变量 ORACLE_SCHEMA 读取）")
    parser.add_argument("--tables", type=str, default="*",
                        help="要同步的表，逗号分隔或 * 表示全部（默认: *）")
    
    args = parser.parse_args()
    
    # 智能同步模式
    if args.smart:
        from hpf_platform.etl.smart_sync import smart_sync
        from hpf_platform.etl.config import ORACLE_CONFIG
        
        # 解析表参数
        if args.tables == "*":
            tables = ["*"]
        else:
            tables = [t.strip() for t in args.tables.split(",")]
        
        schema = args.schema or ORACLE_CONFIG.get("default_schema")
        
        result = smart_sync(
            tables=tables,
            schema=schema,
            approval_mode=not args.auto
        )
        
        if result.get("status") == "success":
            print("\n✅ 智能同步成功完成")
        elif result.get("status") == "partial":
            print("\n⚠️  智能同步部分完成，请检查日志")
        else:
            print(f"\n❌ 智能同步失败: {result.get('message', '未知错误')}")
    
    else:
        # 传统同步模式
        # 从 dlt secrets 读取配置
        oracle_connection = dlt.secrets["sources.oracle_db.credentials"]
        
        # 配置要同步的表
        tables_to_sync = [
            {
                "name": "IM_ZJ_LS",
                "incremental_column": "ID",
                "primary_key": "ID",
                "schema": "SHINEYUE40_BZBGJJYW_CS",  # 添加 schema
                "batch_size": args.batch_size
            }
        ]
        
        # 根据参数选择同步模式
        if args.fast:
            # 高速模式：PyArrow 直接写入
            run_fast_sync(
                oracle_conn=oracle_connection,
                tables=tables_to_sync,
                duckdb_path=args.db,
                num_workers=args.workers,
                use_partition=args.partition  # 传递分区模式参数
            )
        else:
            # 标准模式：dlt pipeline
            run_sync(
                oracle_conn=oracle_connection,
                tables=tables_to_sync,
                duckdb_path=args.db,
                parallel=True,
                num_workers=args.workers
            )

