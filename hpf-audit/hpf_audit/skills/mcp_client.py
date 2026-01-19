"""
MCP Client 封装
用于调用 MCP Servers 的工具函数
"""
import sys
import os
import sqlite3
import json
import re
from typing import Dict, Any, List

# Add mcp-servers path to sys.path to import semantic_mapper
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), "mcp-servers", "hpf-db-adapter"))

try:
    from semantic_mapper import SemanticMapper
    mapper = SemanticMapper()
except ImportError:
    mapper = None
    print("Warning: SemanticMapper not found, schema enrichment disabled.")

class MCPClient:
    """MCP Client 简化版 (Local Direct Call)"""
    
    def __init__(self, db_path: str = "./housing_provident_fund.db"):
        self.db_path = db_path
    
    def list_resources(self, server_name: str) -> List[Dict[str, Any]]:
        """列出资源"""
        if server_name == "hpf-db-adapter":
            try:
                conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
                tables = cursor.fetchall()
                conn.close()
                
                resources = []
                for table in tables:
                    name = table[0]
                    comment = "应用数据表"
                    if mapper:
                        comment = mapper.table_comments.get(name, "应用数据表")
                    
                    resources.append({
                        "uri": f"hpf://schema/tables/{name}",
                        "name": f"{name} ({comment})",
                        "mimeType": "text/x-sql"
                    })
                return resources
            except Exception as e:
                print(f"Error listing resources: {e}")
                return []
        
        return []

    def read_resource(self, server_name: str, uri: str) -> str:
        """读取资源内容"""
        if server_name == "hpf-db-adapter":
            prefix = "hpf://schema/tables/"
            if uri.startswith(prefix):
                table_name = uri[len(prefix):]
                
                try:
                    conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
                    cursor = conn.cursor()
                    cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
                    result = cursor.fetchone()
                    conn.close()
                    
                    if result:
                        raw_ddl = result[0]
                        if mapper:
                            return mapper.enrich_ddl(table_name, raw_ddl)
                        return raw_ddl
                    return f"-- Error: Table {table_name} not found"
                except Exception as e:
                    return f"-- Error reading resource: {e}"
        
        return f"-- Error: Unknown resource {uri}"

    def call(self, server_name: str, tool_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        调用 MCP Server 的工具
        """
        if server_name == "hpf-db-adapter":
            return self._call_db_adapter(tool_name, params)
        elif server_name == "hpf-metadata-store":
            return self._call_metadata_store(tool_name, params)
        else:
            return {"error": f"未知的 MCP Server: {server_name}"}
    
    def _call_db_adapter(self, tool_name: str, params: Dict) -> Dict:
        """调用数据库适配器"""
        
        if tool_name == "safe_query":
            sql = params.get("sql", "")
            mask_data = params.get("mask_data", True)
            
            # 安全检查（允许 SELECT 和 WITH 子句）
            sql_upper = sql.strip().upper()
            if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
                return {"error": "只允许 SELECT 查询"}
            
            try:
                conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute(sql)
                rows = cursor.fetchall()
                conn.close()
                
                data = [dict(row) for row in rows]
                
                # 简单脱敏逻辑 (模拟 server.py)
                mask_data_flag = mask_data
                if mask_data_flag:
                    masked_list = []
                    for row in data:
                        masked_row = {}
                        for k, v in row.items():
                            val_str = str(v) if v is not None else ""
                            # 简单的身份证脱敏
                            if re.match(r'^\d{17}[\dXx]$', val_str):
                                masked_row[k] = val_str[:6] + "********" + val_str[-4:]
                            elif k.lower() in ['name', 'xm', '姓名'] and len(val_str) >= 2:
                                masked_row[k] = val_str[0] + "*" * (len(val_str)-1)
                            else:
                                masked_row[k] = v
                        masked_list.append(masked_row)
                    data = masked_list

                return {
                    "success": True,
                    "row_count": len(data),
                    "data": data
                }
            except Exception as e:
                # 智能错误提示
                error_msg = str(e)
                hint = ""
                if "no such table" in error_msg:
                    hint = "Hint: Table name might be wrong. Please check 'hpf://schema/tables/...' resources."
                return {"error": f"执行失败: {error_msg}", "hint": hint}
        
        elif tool_name == "get_sample_data":
            table_name = params.get("table_name", "")
            try:
                conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
                rows = cursor.fetchall()
                conn.close()
                
                return {
                    "success": True,
                    "data": [dict(row) for row in rows]
                }
            except Exception as e:
                return {"error": str(e)}
        
        return {"error": f"未知工具: {tool_name}"}
    
    def _call_metadata_store(self, tool_name: str, params: Dict) -> Dict:
        """调用元数据存储"""
        # ... existing simplified logic ...
        return {"success": True, "results": []}
