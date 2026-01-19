from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import sqlite3
import json
import os
from typing import List, Optional
from hpf_audit.api.database import get_db_connection, DB_PATH

router = APIRouter()

MCP_SCHEMA_PATH = "mcp-servers/hpf-metadata-store/standard_schema.json"


# Physical Table -> Standard ID OR Chinese Name Mapping
TABLE_ID_MAP = {
    # Key Tables (Map to Standard ID)
    "DW_JC_JBXX": "4.0.2", # 缴存单位
    "GR_JC_JBXX": "4.0.3", # 个人基本信息
    "GR_DK_HT": "6.0.2",   # 个人贷款合同
    
    # Approx/Custom Tables (Map to Name directly)
    "GR_JC_MX": "个人缴存明细",
    "GR_TQ_MX": "个人提取明细",
    "FX_SJ_JL": "风险事件表",
    "GR_DK_HK": "个人贷款还款明细",
    "GT_JKR_XX": "共同借款人信息",
    "GR_DK_CS_JL": "贷款催收记录",
    "GR_DK_YQ": "贷款逾期记录",
    "GR_DK_MX": "贷款发放明细",
    "GR_TQ_YW": "提取业务记录",
    "DW_JC_MX": "单位缴存明细"
}

class ColumnDef(BaseModel):
    name: str # Physical name e.g. DWDZ
    cn_name: str # Chinese name e.g. 单位地址
    type: str # SQL Type e.g. VARCHAR(255)
    length: Optional[str] = "" # Meta length e.g. an..255
    default_value: Optional[str] = None # SQL Default Value

class TableInfo(BaseModel):
    name: str
    columns: List[dict]

def load_standard_schema():
    if not os.path.exists(MCP_SCHEMA_PATH):
        return []
    with open(MCP_SCHEMA_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_standard_schema(data):
    with open(MCP_SCHEMA_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


class NewTableDef(BaseModel):
    """新增表定义"""
    name: str           # 物理表名 e.g. MY_NEW_TABLE
    cn_name: str        # 中文名 e.g. 我的新表
    columns: List[ColumnDef] = []  # 初始字段列表 (可选)


@router.post("/tables")
async def create_table(table: NewTableDef):
    """创建新表并同步到 MCP 标准定义"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 1. 检查表是否已存在
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table.name,))
    if cursor.fetchone():
        conn.close()
        return {"status": 1, "msg": f"表 {table.name} 已存在"}
    
    # 2. 构建 CREATE TABLE SQL
    try:
        column_defs = []
        for col in table.columns:
            col_sql = f"{col.name} {col.type}"
            if col.default_value:
                if col.default_value.isdigit():
                    col_sql += f" DEFAULT {col.default_value}"
                else:
                    col_sql += f" DEFAULT '{col.default_value}'"
            column_defs.append(col_sql)
        
        # 如果没有定义字段，添加一个默认主键
        if not column_defs:
            column_defs.append("id INTEGER PRIMARY KEY AUTOINCREMENT")
        
        sql = f"CREATE TABLE {table.name} ({', '.join(column_defs)})"
        print(f"[Schema] Executing: {sql}")
        cursor.execute(sql)
        conn.commit()
    except Exception as e:
        conn.close()
        return {"status": 1, "msg": f"创建表失败: {str(e)}"}
    
    conn.close()
    
    # 3. 更新 TABLE_ID_MAP (运行时)
    TABLE_ID_MAP[table.name] = table.cn_name
    
    # 4. 同步到 MCP 标准定义文件
    try:
        data = load_standard_schema()
        
        # 生成新的标准表 ID (使用自定义格式 custom.xxx)
        new_id = f"custom.{table.name.lower()}"
        
        # 构建字段列表
        fields = []
        for col in table.columns:
            fields.append({
                "code": col.name,
                "name": col.cn_name,
                "type": "字符型" if "VARCHAR" in col.type or "TEXT" in col.type else ("数字型" if "INT" in col.type or "DECIMAL" in col.type else "字符型"),
                "len": col.length or ""
            })
        
        # 添加新表定义
        new_table_def = {
            "name": table.cn_name,
            "id": new_id,
            "page": 999,  # 自定义表
            "fields": fields,
            "source": "Schema Editor"
        }
        data.append(new_table_def)
        save_standard_schema(data)
        
    except Exception as e:
        return {"status": 0, "msg": f"表已创建，但同步 MCP 失败: {str(e)}"}
    
    # 5. 刷新 Schema 缓存
    try:
        from hpf_audit.utils.schema_loader import refresh_cache
        refresh_cache()
    except Exception:
        pass
    
    return {"status": 0, "msg": f"表 {table.name} 创建成功"}

@router.get("/tables")
async def list_tables():
    """List all tables in SQLite (AMIS Format)"""
    conn = get_db_connection()
    cursor = conn.cursor()
    # User requested excluding META_ and sqlite_
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'META_%'")
    tables = [row['name'] for row in cursor.fetchall()]
    conn.close()
    
    # Load Standard Schema to find Chinese names
    std_data = load_standard_schema()
    
    items = []
    for t_name in tables:
        cn_name = ""
        # 1. Try Map
        std_id = TABLE_ID_MAP.get(t_name)
        if std_id:
            # Check if it's a Standard ID (digits/dots) or a Direct Name
            if any(char.isdigit() for char in std_id) and "." in std_id:
                std_t = next((t for t in std_data if t['id'] == std_id), None)
                if std_t:
                    cn_name = std_t.get("name", "")
            else:
                # It's a direct name
                cn_name = std_id
        
        # 2. If no map, maybe the physical name IS the ID or Name? (Heuristic)
        if not cn_name:
             std_t_by_name = next((t for t in std_data if t.get("name") == t_name or t.get("id") == t_name), None)
             if std_t_by_name:
                 cn_name = std_t_by_name.get("name", "")

        items.append({
            "name": t_name,
            "cn_name": cn_name
        })

    return {
        "status": 0,
        "msg": "ok",
        "data": {
            "items": items
        }
    }

@router.get("/tables/{table_name}")
async def get_table_details(table_name: str):
    """Get columns (AMIS Format)"""
    # 1. Get Physical Columns
    conn = get_db_connection()
    cursor = conn.cursor()
    column_map = {}
    try:
        cursor.execute(f"PRAGMA table_info({table_name})")
        rows = cursor.fetchall()
        for r in rows:
            # cid, name, type, notnull, dflt_value, pk
            column_map[r['name']] = {
                "name": r['name'],
                "type": r['type'],
                "in_db": True,
                "in_standard": False,
                "cn_name": "", # Will fill from standard
                "meta_type": "",
                "meta_len": ""
            }
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Table not found: {e}")
    finally:
        conn.close()

    # 2. Merge with Standard Schema
    standard_data = load_standard_schema()
    standard_table = next((t for t in standard_data if t.get('id') == table_name or t.get('name') == table_name or (table_name == "DW_JC_JBXX" and "4.0.2" in t.get("id", ""))), None)
    
    # Heuristic mapping for Demo Tables -> Standard Tables if ID doesn't match directly
    # In a real system, we'd store a mapping. Here we guess or assume exact match if possible.
    # Currently: DW_JC_JBXX maps to 4.0.2? Let's check IDs.
    # Our DB tables are English (DW_JC_JBXX), Standard table IDs are "4.0.2".
    # Attempt to find by parsing standard fields or name map.
    
    mapped_std_fields = []
    if standard_table: 
        mapped_std_fields = standard_table.get('fields', [])
    else:
        # Fallback: Search all tables for one that contains our columns?
        # Or just return DB info.
        pass

    # Update map with Standard Info
    for f in mapped_std_fields:
        code = f.get('code')
        if code in column_map:
            column_map[code]['in_standard'] = True
            column_map[code]['cn_name'] = f.get('name')
            column_map[code]['meta_type'] = f.get('type')
            column_map[code]['meta_len'] = f.get('len')
        else:
             # Field exists in Standard but NOT in DB
             column_map[code] = {
                "name": code,
                "type": "",
                "in_db": False, # Missing in DB
                "in_standard": True,
                "cn_name": f.get('name'),
                "meta_type": f.get('type'),
                "meta_len": f.get('len')
             }
             
    return {
        "status": 0,
        "msg": "ok",
        "data": {
            "items": list(column_map.values()),
            "standard_id": standard_table.get('id') if standard_table else None
        }
    }

@router.post("/tables/{table_name}/columns")
async def add_column(table_name: str, col: ColumnDef):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 1. Add to SQLite
    try:
        # Sanitize input slightly (simple whitelist or check)
        # In a real app, use better SQL construction logic
        default_clause = ""
        if col.default_value:
            # Handle int vs string defaults
            if col.default_value.isdigit():
                 default_clause = f" DEFAULT {col.default_value}"
            else:
                 default_clause = f" DEFAULT '{col.default_value}'"

        sql = f"ALTER TABLE {table_name} ADD COLUMN {col.name} {col.type}{default_clause}"
        print(f"Executing: {sql}")
        cursor.execute(sql)
        conn.commit()
    except Exception as e:
        conn.close()
        return {"status": 1, "msg": f"Database Error: {str(e)}", "data": None}
    conn.close()
    
    # 2. Update Standard JSON
    try:
        data = load_standard_schema()
        
        std_id = TABLE_ID_MAP.get(table_name)
        target_table = None
        
        if std_id:
             target_table = next((t for t in data if t.get('id') == std_id), None)
        
        if target_table:
            # Check if exists
            exists = next((c for c in target_table.get('columns', []) if c['id'] == col.name), None)
            if not exists:
                target_table.setdefault('columns', []).append({
                    "id": col.name,
                    "name": col.cn_name,
                    "type": "字符型", # Defaulting to string for now, could infer from SQL type
                    "domain": col.length or "",
                    "description": f"Added via Schema Editor. Default: {col.default_value or 'None'}"
                })
                save_standard_schema(data)
                
    except Exception as e:
        return {"status": 0, "msg": f"Column added to DB, but failed to sync Standard: {str(e)}", "data": None}

    return {"status": 0, "msg": f"Column {col.name} added successfully", "data": None}

@router.delete("/tables/{table_name}/columns/{column_name}")
async def drop_column(table_name: str, column_name: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 1. Drop from SQLite
    try:
        # SQLite drop column support is limited but available in newer versions
        cursor.execute(f"ALTER TABLE {table_name} DROP COLUMN {column_name}")
        conn.commit()
    except Exception as e:
        conn.close()
        return {"status": 1, "msg": f"Database Error: {str(e)} (Ensure SQLite >= 3.35)"}
    conn.close()

    # 2. Update Standard JSON
    try:
        data = load_standard_schema()
        target_table = None
        std_id = TABLE_ID_MAP.get(table_name)
        if std_id:
             target_table = next((t for t in data if t.get('id') == std_id), None)
        
        if target_table and 'columns' in target_table:
             target_table['columns'] = [c for c in target_table['columns'] if c['id'] != column_name]
             save_standard_schema(data)
             
    except Exception as e:
         return {"status": 0, "msg": f"Column deleted from DB, but failed to sync Standard: {str(e)}"}

    return {"status": 0, "msg": f"Column {column_name} deleted successfully"}
