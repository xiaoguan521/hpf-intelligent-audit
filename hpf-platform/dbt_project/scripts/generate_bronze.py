#!/usr/bin/env python3
"""
AI Bronze Generator
自动读取 DuckDB 中的 raw data (oracle_data schema)
并使用 LLM 生成 dbt Bronze 层的 schema.yml 和 .sql 文件
"""
import os
import sys
import yaml
import logging

# Add project root and hpf-common to path
# Script is at: hpf-platform/dbt_project/scripts/generate_bronze.py
# Root is at:   ../../../
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.append(project_root)
sys.path.append(os.path.join(project_root, "hpf-common"))

from hpf_common.db import DBManager
from hpf_common.llm import LLMClient
from hpf_common.config import settings

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("BronzeGen")

# Constants
# Models dir is at: ../models/bronze
DBT_BRONZE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../models/bronze"))
SOURCE_SCHEMA = "oracle_data"  # DuckDB source schema

def get_table_metadata():
    # Calculate absolute path to DB based on project root
    # DB is located at: <project_root>/hpf-platform/data/warehouse.duckdb
    db_path = os.path.join(project_root, "hpf-platform/data/warehouse.duckdb")
    logger.info(f"Connecting to DuckDB: {db_path}")
    
    tables_meta = {}
    
    with DBManager.connect("duckdb", path=db_path, read_only=True) as conn:
        # 获取所有表
        query_tables = f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{SOURCE_SCHEMA}'
        """
        tables = [row[0] for row in conn.execute(query_tables).fetchall()]
        
        # 获取每个表的列
        for table in tables:
            query_cols = f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = '{SOURCE_SCHEMA}' AND table_name = '{table}'
            """
            columns = conn.execute(query_cols).fetchall()
            tables_meta[table] = [{"name": c[0], "type": c[1]} for c in columns]
            
    return tables_meta

def generate_dbt_files(table_name, columns, llm_client):
    """
    使用 LLM 生成 dbt 文件内容
    1. schema.yml 片段 (Table description)
    2. .sql 文件名和内容
    """
    logger.info(f"Generating content for table: {table_name}")
    
    prompt = f"""
    我是一个 dbt 工程师。我有一个原始表 `{SOURCE_SCHEMA}.{table_name}`。
    列信息如下:
    {columns}
    
    请帮我完成两个任务:
    
    Task 1: 为该表生成一个合理的 dbt Bronze 层文件名 (通常是 src_xxx.sql)。
    Task 2: 为该表生成中文描述 (description)。
    Task 3: 生成该表的 dbt SQL 代码 (很简单, 就是 select * from source)。
    
    请严格按照以下 JSON 格式返回 (不要包含 Markdown 代码块):
    {{
        "filename": "src_example.sql",
        "description": "这是示例表的中文描述",
        "sql_content": "select * from {{ source('oracle_data', 'EXAMPLE_TABLE') }}"
    }}
    """
    
    try:
        # 使用 json mode 或者是简单的 parse
        messages = [{"role": "user", "content": prompt}]
        response = llm_client.chat(messages)
        
        # 简单的清洗，防止 LLM 返回 markdown block
        clean_resp = response.replace("```json", "").replace("```", "").strip()
        import json
        return json.loads(clean_resp)
        
    except Exception as e:
        logger.warning(f"LLM generation failed for {table_name}: {e}. Using fallback template.")
        # Fallback template
        return {
            "filename": f"src_{table_name.lower()}.sql",
            "description": f"Source table {table_name} from oracle_data",
            "sql_content": f"select * from {{{{ source('{SOURCE_SCHEMA}', '{table_name}') }}}}"
        }

def main():
    if not os.path.exists(DBT_BRONZE_DIR):
        os.makedirs(DBT_BRONZE_DIR)
        
    # 1. 获取元数据
    try:
        tables_meta = get_table_metadata()
    except Exception as e:
        logger.error(f"Failed to get metadata: {e}")
        return

    if not tables_meta:
        logger.warning(f"No tables found in schema '{SOURCE_SCHEMA}'. Initialize mock data first?")
        return
        
    logger.info(f"Found {len(tables_meta)} tables: {list(tables_meta.keys())}")
    
    # 2. 初始化 LLM
    llm = LLMClient()
    
    # 3. 准备 schema.yml 结构
    schema_yaml = {
        "version": 2,
        "sources": [{
            "name": SOURCE_SCHEMA,
            "schema": SOURCE_SCHEMA,
            "tables": []
        }]
    }
    
    # 4. 遍历表生成文件
    for table_name, cols in tables_meta.items():
        result = generate_dbt_files(table_name, cols, llm)
        if not result:
            continue
            
        # Add to schema.yaml
        table_def = {
            "name": table_name,
            "description": result['description']
        }
        schema_yaml['sources'][0]['tables'].append(table_def)
        
        # Write .sql file
        file_path = os.path.join(DBT_BRONZE_DIR, result['filename'])
        with open(file_path, 'w') as f:
            f.write(result['sql_content'])
        logger.info(f"Created file: {result['filename']}")
        
    # 5. Write schema.yml
    schema_path = os.path.join(DBT_BRONZE_DIR, "schema.yml")
    with open(schema_path, 'w') as f:
        yaml.dump(schema_yaml, f, allow_unicode=True, sort_keys=False)
    logger.info(f"Updated schema.yml at {schema_path}")
    
    logger.info("🎉 AI Bronze Generation Complete!")

if __name__ == "__main__":
    main()
