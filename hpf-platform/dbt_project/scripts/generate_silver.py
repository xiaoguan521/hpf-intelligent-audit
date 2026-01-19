#!/usr/bin/env python3
"""
AI Silver Generator
自动读取 dbt Bronze 层的 Views (DuckDB)
并使用 LLM 生成 Silver 层的清洗逻辑 (stg_xxx.sql)
"""
import os
import sys
import yaml
import logging

# Add project root and hpf-common to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.append(project_root)
sys.path.append(os.path.join(project_root, "hpf-common"))

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    env_path = os.path.join(project_root, ".env")
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print(f"Loaded environment from {env_path}")
    else:
        print(f"Warning: .env file not found at {env_path}")
except ImportError:
    print("Warning: python-dotenv not installed. Environment variables might not be loaded.")

from hpf_common.db import DBManager
from hpf_common.llm import LLMClient
from hpf_common.config import settings

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("SilverGen")

# Constants
DBT_SILVER_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../models/silver"))
DBT_PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

def get_bronze_models():
    """读取 Bronze 层已有的模型列表"""
    bronze_dir = os.path.join(DBT_PROJECT_DIR, "models/bronze")
    models = []
    for f in os.listdir(bronze_dir):
        if f.endswith(".sql") and f.startswith("src_"):
            models.append(f.replace(".sql", ""))
    return models

def get_table_sample(table_name):
    """获取 Bronze 表的 Schema 和 样本数据"""
    # DuckDB Path
    db_path = os.path.join(project_root, "hpf-platform/data/warehouse.duckdb")
    
    with DBManager.connect("duckdb", path=db_path, read_only=True) as conn:
        # 1. Get Columns
        try:
            # 1.1 Find schema first (dbt might put views in 'analytics' or 'main')
            schema_query = f"SELECT table_schema FROM information_schema.tables WHERE table_name = '{table_name}' LIMIT 1"
            res = conn.execute(schema_query).fetchone()
            
            if res:
                full_table_name = f"{res[0]}.{table_name}"
                logger.info(f"Found table {table_name} in schema {res[0]}")
            else:
                # Try explicit 'analytics' fallback or default
                full_table_name = f"analytics.{table_name}"
                logger.warning(f"Schema not found for {table_name}, trying {full_table_name}")

            query_sample = f"SELECT * FROM {full_table_name} LIMIT 3"
            df = conn.execute(query_sample).df()
            return df
        except Exception as e:
            logger.warning(f"Could not read table {table_name}: {e}")
            return None

def generate_silver_file(model_name, sample_df, llm_client):
    """使用 LLM 生成 Silver SQL"""
    logger.info(f"Generating Silver logic for: {model_name}")
    
    # 构造 Prompt
    columns_info = []
    for col in sample_df.columns:
        # 获取前几个非空值作为样本
        samples = sample_df[col].dropna().head(3).tolist()
        columns_info.append(f"- {col}: {samples}")
    
    col_str = "\n".join(columns_info)
    
    prompt = f"""
    我是一个 dbt 工程师。我有一个 Bronze 层的基础表 `{model_name}`。
    列名和样本数据如下:
    {col_str}
    
    请编写一个 Silver 层的清洗 SQL (文件名通常为 `stg_xxx.sql`):
    
    要求:
    1. **Renaming**: 将晦涩的列名重命名为清晰的英文名 (例如 loan_amt -> loan_amount)。
    2. **Casting**: 将金额转为 decimal, IDs 转 string, status 保持 string。
    3. **Mapping**: 如果发现 status 列有 '01', '02', '1', '2' 等值, 请尝试用 Case When 映射为 'active'/'normal', 'overdue' 等易读状态。
    4. **Source**: 使用 `{{ ref('{model_name}') }}` 引用源表。
    
    ps:
    - Return RAW JSON only. No backticks wrapping values.
    - SQL content string must be double quoted and escaped properly (e.g. use \\n for newlines).
    - Do not use ` (backtick) for quoting strings.
    
    Example:
    {{
        "filename": "...",
        "sql_content": "SELECT * \\n FROM table" 
    }}
    """
    
    import re
    import json
    
    try:
        messages = [{"role": "user", "content": prompt}]
        response = llm_client.chat(messages)
        
        # Robust JSON extraction
        match = re.search(r"```(?:json)?\s*(.*?)\s*```", response, re.DOTALL)
        if match:
            clean_resp = match.group(1).strip()
        else:
            # Try to find the first { and last }
            start = response.find("{")
            end = response.rfind("}")
            if start != -1 and end != -1:
                clean_resp = response[start:end+1]
            else:
                clean_resp = response.strip()

        # Fix: Replace backticks used as quotes for sql_content with double quotes
        # Pattern: "key": `value` -> "key": "value"
        # Since value might contain newlines, we need to be careful.
        # But simple fix: if we see ` at start of value, replace with " and escape internal "
        
        # Method 2: Use dirtyjson if available (stronger)
        try:
            import dirtyjson
            return dirtyjson.loads(clean_resp)
        except ImportError:
            # Manual patch: replace `...` with "..."
            # Note: This is hacky. Better to instruct LLM not to use backticks.
            pass

        # Since we can't easily install dirtyjson, let's fix the prompt!
        # But first, try a simple replace for the specific error
        clean_resp = re.sub(r':\s*`([^`]+)`', lambda m: ': "' + m.group(1).replace('"', '\\"').replace('\n', '\\n') + '"', clean_resp)

        logger.info(f"Parsing JSON content: {clean_resp}") 
        
        return json.loads(clean_resp)
            
    except Exception as e:
        logger.warning(f"LLM generation failed for {model_name}: {e}. Raw response: {response[:200]}...")
        # Fallback
        target_name = model_name.replace("src_", "stg_").replace("gr_dk_", "").replace("gr_", "")
        return {
            "filename": f"{target_name}.sql",
            "description": f"Staging for {model_name}",
            "sql_content": f"select * from {{{{ ref('{model_name}') }}}}"
        }

def main():
    if not os.path.exists(DBT_SILVER_DIR):
        os.makedirs(DBT_SILVER_DIR)
        
    llm = LLMClient()
    models = get_bronze_models()
    
    logger.info(f"Found {len(models)} bronze models: {models}")
    
    for model in models:
        # Get sample data to help LLM understand
        sample_df = get_table_sample(model)
        if sample_df is None:
            continue
            
        result = generate_silver_file(model, sample_df, llm)
        
        # Write File
        file_path = os.path.join(DBT_SILVER_DIR, result['filename'])
        # 简单防重: 如果文件已存在且非空, 也许不该覆盖? 
        # 这里为了演示方便, 我们直接覆盖
        with open(file_path, 'w') as f:
            f.write(result['sql_content'])
        logger.info(f"Created {result['filename']}")

    logger.info("🎉 Silver Layer Generation Complete")

if __name__ == "__main__":
    main()
