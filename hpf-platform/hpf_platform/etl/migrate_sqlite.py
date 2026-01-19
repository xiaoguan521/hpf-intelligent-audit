"""
SQLite to DuckDB 数据迁移脚本
=============================
将现有 SQLite 数据库完整迁移到 DuckDB
"""
import duckdb
import sqlite3
import os
import sys

# 添加项目根目录
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from hpf_platform.etl.config import DUCKDB_PATH


def migrate_sqlite_to_duckdb(
    sqlite_path: str = "./housing_provident_fund.db",
    duckdb_path: str = None,
    schema_name: str = "main"
):
    """
    将 SQLite 数据库完整迁移到 DuckDB
    
    Args:
        sqlite_path: SQLite 数据库路径
        duckdb_path: DuckDB 数据库路径（默认使用配置）
        schema_name: 目标 schema 名称
    """
    if duckdb_path is None:
        duckdb_path = DUCKDB_PATH
    
    # 确保目标目录存在
    os.makedirs(os.path.dirname(duckdb_path) if os.path.dirname(duckdb_path) else ".", exist_ok=True)
    
    print("\n" + "=" * 60)
    print("🔄 SQLite → DuckDB 数据迁移")
    print("=" * 60)
    print(f"   源数据库: {sqlite_path}")
    print(f"   目标数据库: {duckdb_path}")
    print("=" * 60)
    
    # 连接 SQLite
    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_cursor = sqlite_conn.cursor()
    
    # 连接 DuckDB
    duck_conn = duckdb.connect(duckdb_path)
    
    # 安装并加载 sqlite 扩展
    print("\n📦 安装 SQLite 扩展...")
    duck_conn.execute("INSTALL sqlite")
    duck_conn.execute("LOAD sqlite")
    print("   ✅ SQLite 扩展已加载")
    
    # 获取所有表名
    sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    tables = [row[0] for row in sqlite_cursor.fetchall()]
    
    print(f"\n📋 发现 {len(tables)} 张表：")
    for t in tables:
        print(f"   - {t}")
    
    # 迁移每张表
    migrated_count = 0
    for table_name in tables:
        try:
            print(f"\n⏳ 正在迁移表: {table_name}...")
            
            # 获取表结构
            sqlite_cursor.execute(f"PRAGMA table_info('{table_name}')")
            columns = sqlite_cursor.fetchall()
            
            # 获取数据
            sqlite_cursor.execute(f"SELECT * FROM '{table_name}'")
            rows = sqlite_cursor.fetchall()
            
            if not rows:
                print(f"   ⚠️  表 {table_name} 为空，跳过")
                continue
            
            # 在 DuckDB 中创建表（使用 SQLite 扩展直接复制）
            # 先删除已存在的表
            duck_conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            # 使用 DuckDB 的 SQLite 扫描功能直接复制
            try:
                duck_conn.execute(f"""
                    CREATE TABLE {table_name} AS 
                    SELECT * FROM sqlite_scan('{sqlite_path}', '{table_name}')
                """)
            except duckdb.Error as type_err:
                if "Mismatch Type Error" in str(type_err):
                    # 类型不匹配，使用 sqlite_all_varchar 模式重试
                    print(f"   ⚠️  类型不匹配，使用 VARCHAR 模式重试...")
                    duck_conn.execute("SET sqlite_all_varchar=true")
                    duck_conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                    duck_conn.execute(f"""
                        CREATE TABLE {table_name} AS 
                        SELECT * FROM sqlite_scan('{sqlite_path}', '{table_name}')
                    """)
                    duck_conn.execute("SET sqlite_all_varchar=false")
                else:
                    raise
            
            # 验证行数
            result = duck_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            row_count = result[0]
            
            print(f"   ✅ 迁移完成: {row_count} 行")
            migrated_count += 1
            
        except Exception as e:
            print(f"   ❌ 迁移失败: {e}")
    
    # 关闭连接
    sqlite_conn.close()
    duck_conn.close()
    
    print("\n" + "=" * 60)
    print(f"✅ 迁移完成！共迁移 {migrated_count}/{len(tables)} 张表")
    print(f"   DuckDB 文件: {duckdb_path}")
    print("=" * 60 + "\n")
    
    return migrated_count


def verify_migration(
    sqlite_path: str = "./housing_provident_fund.db",
    duckdb_path: str = None
):
    """验证迁移结果"""
    if duckdb_path is None:
        duckdb_path = DUCKDB_PATH
    
    print("\n🔍 验证迁移结果...")
    
    sqlite_conn = sqlite3.connect(sqlite_path)
    duck_conn = duckdb.connect(duckdb_path, read_only=True)
    
    sqlite_cursor = sqlite_conn.cursor()
    sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    tables = [row[0] for row in sqlite_cursor.fetchall()]
    
    print(f"\n{'表名':<30} {'SQLite':<15} {'DuckDB':<15} {'状态':<10}")
    print("-" * 70)
    
    all_match = True
    for table_name in tables:
        try:
            sqlite_cursor.execute(f"SELECT COUNT(*) FROM '{table_name}'")
            sqlite_count = sqlite_cursor.fetchone()[0]
            
            duck_count = duck_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            
            status = "✅" if sqlite_count == duck_count else "❌"
            if sqlite_count != duck_count:
                all_match = False
            
            print(f"{table_name:<30} {sqlite_count:<15} {duck_count:<15} {status:<10}")
        except Exception as e:
            print(f"{table_name:<30} {'N/A':<15} {'Error':<15} ❌")
            all_match = False
    
    sqlite_conn.close()
    duck_conn.close()
    
    print("\n" + ("✅ 所有表验证通过！" if all_match else "⚠️  部分表验证失败"))
    return all_match


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="SQLite to DuckDB Migration")
    parser.add_argument("--sqlite", default="./housing_provident_fund.db", help="SQLite 数据库路径")
    parser.add_argument("--duckdb", default=None, help="DuckDB 数据库路径")
    parser.add_argument("--verify-only", action="store_true", help="仅验证，不迁移")
    
    args = parser.parse_args()
    
    if args.verify_only:
        verify_migration(args.sqlite, args.duckdb)
    else:
        migrate_sqlite_to_duckdb(args.sqlite, args.duckdb)
        verify_migration(args.sqlite, args.duckdb)
