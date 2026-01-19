"""
数据同步校验器
=============
对比源库和目标库的数据，确保同步完整性
"""
import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum

import duckdb
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


class VerifyStatus(Enum):
    """校验状态"""
    SUCCESS = "success"
    MISMATCH = "mismatch"
    ERROR = "error"


@dataclass
class VerifyResult:
    """校验结果"""
    table_name: str
    status: VerifyStatus
    source_count: int
    target_count: int
    difference: int
    message: str
    
    @property
    def is_success(self) -> bool:
        return self.status == VerifyStatus.SUCCESS


class SyncVerifier:
    """同步数据校验器"""
    
    def __init__(
        self, 
        oracle_conn_string: str, 
        duckdb_path: str,
        dataset_name: str = "oracle_data"
    ):
        """
        初始化校验器
        
        Args:
            oracle_conn_string: Oracle 连接字符串
            duckdb_path: DuckDB 数据库路径
            dataset_name: DuckDB 中的 schema 名称
        """
        self.oracle_conn_string = oracle_conn_string
        self.duckdb_path = duckdb_path
        self.dataset_name = dataset_name
        self._oracle_engine = None
        self._duck_conn = None
    
    @property
    def oracle_engine(self):
        """懒加载 Oracle 引擎"""
        if self._oracle_engine is None:
            self._oracle_engine = create_engine(
                self.oracle_conn_string,
                pool_size=2,
                max_overflow=1,
                pool_pre_ping=True,
                echo=False
            )
        return self._oracle_engine
    
    @property
    def duck_conn(self):
        """懒加载 DuckDB 连接"""
        if self._duck_conn is None:
            self._duck_conn = duckdb.connect(self.duckdb_path, read_only=True)
        return self._duck_conn
    
    def get_oracle_count(self, table_name: str, schema: str) -> int:
        """获取 Oracle 表行数"""
        # 如果表名已经包含 . (例如 SCHEMA.TABLE)，则不再拼接 schema
        if "." in table_name:
            full_table = table_name
        else:
            full_table = f"{schema}.{table_name}"
            
        query = f"SELECT COUNT(*) FROM {full_table}"
        
        with self.oracle_engine.connect() as conn:
            result = conn.execute(text(query))
            return result.fetchone()[0]
    
    def get_duckdb_count(self, table_name: str) -> int:
        """获取 DuckDB 表行数"""
        # 剥离可能的 schema 前缀（避免 catalog.schema.table 错误）
        if "." in table_name:
            pure_name = table_name.split(".")[-1]
        else:
            pure_name = table_name
        full_table = f"{self.dataset_name}.{pure_name}"
        try:
            result = self.duck_conn.execute(f"SELECT COUNT(*) FROM {full_table}")
            return result.fetchone()[0]
        except Exception as e:
            logger.warning(f"DuckDB 表 {full_table} 不存在或查询失败: {e}")
            return -1
    
    def verify_row_count(
        self, 
        table_name: str, 
        schema: str,
        tolerance: float = 0.0
    ) -> VerifyResult:
        """
        验证行数一致性
        
        Args:
            table_name: 表名
            schema: Oracle schema
            tolerance: 允许的误差比例（0.0 = 完全一致）
            
        Returns:
            VerifyResult 对象
        """
        try:
            source_count = self.get_oracle_count(table_name, schema)
            target_count = self.get_duckdb_count(table_name)
            
            if target_count < 0:
                return VerifyResult(
                    table_name=table_name,
                    status=VerifyStatus.ERROR,
                    source_count=source_count,
                    target_count=0,
                    difference=source_count,
                    message=f"目标表 {table_name} 不存在"
                )
            
            difference = abs(source_count - target_count)
            
            # 计算误差比例
            if source_count > 0:
                error_rate = difference / source_count
            else:
                error_rate = 0 if target_count == 0 else 1.0
            
            if error_rate <= tolerance:
                return VerifyResult(
                    table_name=table_name,
                    status=VerifyStatus.SUCCESS,
                    source_count=source_count,
                    target_count=target_count,
                    difference=difference,
                    message=f"校验通过: 源 {source_count:,} 行, 目标 {target_count:,} 行"
                )
            else:
                return VerifyResult(
                    table_name=table_name,
                    status=VerifyStatus.MISMATCH,
                    source_count=source_count,
                    target_count=target_count,
                    difference=difference,
                    message=f"行数不一致: 源 {source_count:,} 行, 目标 {target_count:,} 行, 差异 {difference:,}"
                )
                
        except Exception as e:
            logger.error(f"校验表 {table_name} 失败: {e}")
            return VerifyResult(
                table_name=table_name,
                status=VerifyStatus.ERROR,
                source_count=0,
                target_count=0,
                difference=0,
                message=f"校验错误: {str(e)}"
            )
    
    def verify_multiple_tables(
        self, 
        tables: list,
        schema: str,
        tolerance: float = 0.0,
        progress_callback=None
    ) -> Dict[str, VerifyResult]:
        """
        验证多个表
        
        Args:
            tables: 表名列表
            schema: Oracle schema
            tolerance: 允许的误差比例
            progress_callback: 进度回调 (current, total, table_name)
            
        Returns:
            {table_name: VerifyResult} 字典
        """
        results = {}
        total = len(tables)
        
        for i, table_name in enumerate(tables):
            if progress_callback:
                progress_callback(i + 1, total, table_name)
            
            results[table_name] = self.verify_row_count(table_name, schema, tolerance)
        
        return results
    
    def get_summary(self, results: Dict[str, VerifyResult]) -> Dict[str, Any]:
        """
        获取校验汇总
        
        Args:
            results: 校验结果字典
            
        Returns:
            汇总信息
        """
        success_count = sum(1 for r in results.values() if r.status == VerifyStatus.SUCCESS)
        mismatch_count = sum(1 for r in results.values() if r.status == VerifyStatus.MISMATCH)
        error_count = sum(1 for r in results.values() if r.status == VerifyStatus.ERROR)
        
        total_source = sum(r.source_count for r in results.values())
        total_target = sum(r.target_count for r in results.values() if r.target_count >= 0)
        
        return {
            "total_tables": len(results),
            "success": success_count,
            "mismatch": mismatch_count,
            "error": error_count,
            "total_source_rows": total_source,
            "total_target_rows": total_target,
            "all_passed": mismatch_count == 0 and error_count == 0
        }
    
    def close(self):
        """关闭连接"""
        if self._oracle_engine:
            self._oracle_engine.dispose()
            self._oracle_engine = None
        if self._duck_conn:
            self._duck_conn.close()
            self._duck_conn = None


# 测试入口
if __name__ == "__main__":
    from hpf_platform.etl.config import get_oracle_connection_string, DUCKDB_PATH
    
    verifier = SyncVerifier(
        oracle_conn_string=get_oracle_connection_string(),
        duckdb_path=DUCKDB_PATH,
        dataset_name="oracle_data"
    )
    
    # 测试单表校验
    result = verifier.verify_row_count("IM_ZJ_LS", "SHINEYUE40_BZBGJJYW_CS")
    print(f"校验结果: {result.status.value}")
    print(f"  {result.message}")
    
    verifier.close()
