"""
Oracle to DuckDB ETL Pipeline
==============================
使用 dlt 实现从 Oracle 到 DuckDB 的数据同步：
- 增量加载（基于 UPDATE_TIME）
- Merge/Upsert（基于 Primary Key）
- ODS 层 1:1 镜像
"""
import dlt
from dlt.sources.sql_database import sql_database
import pendulum
from typing import Optional
import os
import sys

# 添加项目根目录到 path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from hpf_platform.etl.config import (
    get_oracle_connection_string,
    DUCKDB_PATH,
    ODS_TABLES,
    PIPELINE_CONFIG,
)


def create_oracle_source(table_names: Optional[list] = None):
    """
    创建 Oracle 数据源
    
    Args:
        table_names: 指定要同步的表名列表，None 表示同步所有配置的表
    
    Returns:
        dlt source 对象
    """
    # 获取要同步的表名
    if table_names is None:
        table_names = [t["table_name"] for t in ODS_TABLES]
    
    # 创建 sql_database 源
    source = sql_database(
        credentials=get_oracle_connection_string(),
        table_names=table_names,
    )
    
    # 为每个表应用增量加载配置
    for table_config in ODS_TABLES:
        table_name = table_config["table_name"]
        if table_name not in table_names:
            continue
            
        # 获取表资源
        resource = getattr(source, table_name, None)
        if resource is None:
            print(f"⚠️  表 {table_name} 未找到，跳过")
            continue
        
        # 应用增量加载提示
        initial_value = pendulum.parse(PIPELINE_CONFIG["default_initial_value"])
        resource.apply_hints(
            incremental=dlt.sources.incremental(
                table_config["incremental_field"],
                initial_value=initial_value,
                range_start="open",  # 开区间，避免重复加载边界数据
            ),
            primary_key=table_config["primary_key"],
        )
        
        print(f"✅ 配置表 {table_name}: "
              f"增量字段={table_config['incremental_field']}, "
              f"主键={table_config['primary_key']}")
    
    return source


def create_pipeline():
    """
    创建 dlt Pipeline
    
    Returns:
        dlt pipeline 对象
    """
    # 确保数据目录存在
    data_dir = os.path.dirname(DUCKDB_PATH)
    if data_dir and not os.path.exists(data_dir):
        os.makedirs(data_dir)
        print(f"📁 创建数据目录: {data_dir}")
    
    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_CONFIG["pipeline_name"],
        destination=dlt.destinations.duckdb(DUCKDB_PATH),
        dataset_name=PIPELINE_CONFIG["dataset_name"],
    )
    
    print(f"🔧 Pipeline 创建完成: {PIPELINE_CONFIG['pipeline_name']}")
    print(f"   目标: {DUCKDB_PATH}")
    print(f"   Schema: {PIPELINE_CONFIG['dataset_name']}")
    
    return pipeline


def run_sync(table_names: Optional[list] = None, full_refresh: bool = False):
    """
    执行数据同步
    
    Args:
        table_names: 指定要同步的表名列表，None 表示同步所有配置的表
        full_refresh: 是否全量刷新（True=replace, False=merge）
    
    Returns:
        load_info 对象
    """
    print("\n" + "=" * 60)
    print("🚀 开始 Oracle → DuckDB 数据同步")
    print("=" * 60)
    
    # 创建源和管道
    source = create_oracle_source(table_names)
    pipeline = create_pipeline()
    
    # 确定写入模式
    write_disposition = "replace" if full_refresh else "merge"
    print(f"\n📝 写入模式: {write_disposition}")
    
    # 执行同步
    print("\n⏳ 正在同步数据...")
    load_info = pipeline.run(
        source,
        write_disposition=write_disposition,
    )
    
    # 打印结果
    print("\n" + "=" * 60)
    print("✅ 同步完成！")
    print("=" * 60)
    print(load_info)
    
    return load_info


def run_single_table_sync(table_name: str, full_refresh: bool = False):
    """
    同步单张表
    
    Args:
        table_name: 表名
        full_refresh: 是否全量刷新
    """
    return run_sync(table_names=[table_name], full_refresh=full_refresh)


# ============================================================
# CLI 入口
# ============================================================
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Oracle to DuckDB ETL Pipeline")
    parser.add_argument(
        "--tables",
        nargs="+",
        default=None,
        help="指定要同步的表名，不指定则同步所有配置的表",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="全量刷新（替换现有数据）",
    )
    parser.add_argument(
        "--list-tables",
        action="store_true",
        help="列出所有配置的表",
    )
    
    args = parser.parse_args()
    
    if args.list_tables:
        print("\n📋 配置的 ODS 表：")
        for i, t in enumerate(ODS_TABLES, 1):
            print(f"   {i}. {t['table_name']} - {t['description']}")
        print()
    else:
        run_sync(table_names=args.tables, full_refresh=args.full_refresh)
