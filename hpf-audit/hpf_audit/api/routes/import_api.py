"""
数据导入 API 路由
提供文件上传、导入执行、历史查询等接口
"""
from fastapi import APIRouter, File, UploadFile, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, Dict, List, Union
import os
import tempfile
import sqlite3
from datetime import datetime

from utils.data_importer import DataImporter, save_import_history
# 导入 schema 模块以获取中文名映射
try:
    from hpf_audit.api.routes.schema import TABLE_ID_MAP, load_standard_schema
except ImportError:
    TABLE_ID_MAP = {}
    load_standard_schema = lambda: []

router = APIRouter()

# 临时文件存储目录
UPLOAD_DIR = os.path.join(tempfile.gettempdir(), 'data_imports')
os.makedirs(UPLOAD_DIR, exist_ok=True)


class ImportRequest(BaseModel):
    """导入请求参数"""
    file_id: str
    table_name: str
    field_mapping: Optional[Union[Dict[str, str], List[Dict[str, str]]]] = None


class PrepareRequest(BaseModel):
    """准备导入请求参数"""
    file_id: str
    table_name: str



class ImportResponse(BaseModel):
    """导入响应"""
    success: bool
    message: str
    import_id: Optional[int] = None
    total_rows: int = 0
    success_rows: int = 0
    failed_rows: int = 0
    errors: Optional[List[str]] = None


@router.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """
    上传数据文件
    
    支持的格式: Excel (.xlsx, .xls), CSV (.csv), JSON (.json)
    """
    try:
        # 验证文件格式
        allowed_extensions = ['.xlsx', '.xls', '.csv', '.json']
        file_ext = os.path.splitext(file.filename)[1].lower()
        
        if file_ext not in allowed_extensions:
            raise HTTPException(
                status_code=400,
                detail=f"不支持的文件格式: {file_ext}，仅支持 {', '.join(allowed_extensions)}"
            )
        
        # 生成唯一文件名
        file_id = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{file.filename}"
        file_path = os.path.join(UPLOAD_DIR, file_id)
        
        # 保存文件
        with open(file_path, 'wb') as f:
            content = await file.read()
            f.write(content)
        
        # 预览文件
        importer = DataImporter()
        preview_result = importer.preview_file(file_path, max_rows=5)
        
        if not preview_result['success']:
            os.remove(file_path)
            raise HTTPException(status_code=400, detail=preview_result['error'])
        
        file_info = {
                "file_id": file_id,
                "file_name": file.filename,
                "file_size": os.path.getsize(file_path),
                "file_type": file_ext.replace('.', ''),
                "stats": preview_result['stats'],
                "preview": preview_result['preview']
            }
        
        return {
            "status": 0,
            "msg": "文件上传成功",
            "data": {
                "value": file_info,
                **file_info  # 兼容性:同时也保留外层的字段
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"文件上传失败: {str(e)}")


@router.post("/execute")
async def execute_import(request: ImportRequest):
    """
    执行数据导入
    
    Args:
        file_id: 上传文件的ID
        table_name: 目标表名
        field_mapping: 字段映射 {文件列名: 数据库列名}
    """
    try:
        # 检查文件是否存在
        file_path = os.path.join(UPLOAD_DIR, request.file_id)
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="文件不存在或已过期")
        
        # 规范化字段映射 (处理前端数组格式)
        field_mapping = request.field_mapping
        if isinstance(field_mapping, list):
            normalized_mapping = {}
            for item in field_mapping:
                if isinstance(item, dict) and 'source' in item and 'target' in item:
                    normalized_mapping[item['source']] = item['target']
            field_mapping = normalized_mapping

        # 执行导入
        importer = DataImporter()
        result = importer.import_from_file(
            file_path=file_path,
            table_name=request.table_name,
            field_mapping=field_mapping
        )
        
        # 保存导入历史
        db_path = os.getenv('DB_PATH', 'housing_provident_fund.db')
        import_id = save_import_history(
            db_path=db_path,
            file_name=request.file_id,
            file_size=os.path.getsize(file_path),
            file_type=os.path.splitext(file_path)[1].replace('.', ''),
            target_table=request.table_name,
            field_mapping=request.field_mapping,
            result=result
        )
        
        # 清理临时文件
        try:
            os.remove(file_path)
        except:
            pass
        
        if result['success']:
            return {
                "status": 0,
                "msg": "导入成功",
                "data": {
                    "import_id": import_id,
                    "total_rows": result['total_rows'],
                    "success_rows": result['success_rows'],
                    "failed_rows": result['failed_rows'],
                    "errors": result.get('errors')
                }
            }
        else:
            return {
                "status": 1,
                "msg": f"导入失败: {result.get('error', '未知错误')}",
                "data": {
                    "import_id": import_id,
                    "total_rows": result.get('total_rows', 0),
                    "error": result.get('error')
                }
            }
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"导入失败: {str(e)}")


@router.get("/history")
async def get_import_history(
    page: int = Query(1, ge=1),
    perPage: int = Query(10, ge=1, le=100)
):
    """
    查询导入历史记录
    
    支持分页
    """
    try:
        db_path = os.getenv('DB_PATH', 'housing_provident_fund.db')
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # 确保表存在
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS t_import_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name TEXT NOT NULL,
                file_size INTEGER,
                file_type TEXT,
                target_table TEXT NOT NULL,
                field_mapping TEXT,
                total_rows INTEGER,
                success_rows INTEGER,
                failed_rows INTEGER,
                status TEXT,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 计算总数
        cursor.execute("SELECT COUNT(*) as total FROM t_import_history")
        total = cursor.fetchone()['total']
        
        # 分页查询
        offset = (page - 1) * perPage
        cursor.execute(
            """
            SELECT * FROM t_import_history 
            ORDER BY created_at DESC 
            LIMIT ? OFFSET ?
            """,
            (perPage, offset)
        )
        
        rows = cursor.fetchall()
        conn.close()
        
        return {
            "status": 0,
            "msg": "ok",
            "data": {
                "items": [dict(row) for row in rows],
                "total": total,
                "page": page,
                "perPage": perPage
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询失败: {str(e)}")


@router.get("/tables")
async def get_available_tables():
    """
    获取所有可用的数据库表
    
    用于前端下拉选择
    """
    try:
        db_path = os.getenv('DB_PATH', 'housing_provident_fund.db')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 获取所有表（过滤系统表和风险结果表）
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' 
            AND name NOT LIKE 'sqlite_%' 
            AND name NOT LIKE 'META_%'
            AND name != 'FX_SJ_JL'
            ORDER BY name
        """)
        
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        # 获取中文名映射
        std_data = load_standard_schema()
        
        # 转换为 AMIS 选项格式：中文名 (表名)
        options = []
        for t_name in tables:
            cn_name = ""
            # 1. 尝试从映射表获取
            std_id = TABLE_ID_MAP.get(t_name)
            if std_id:
                # 检查是否为标准ID（数字+点）
                if any(char.isdigit() for char in std_id) and "." in std_id:
                    std_t = next((t for t in std_data if t['id'] == std_id), None)
                    if std_t:
                        cn_name = std_t.get("name", "")
                else:
                    # 直接也就是中文名
                    cn_name = std_id
            
            # 2. 如果没找到，尝试模糊匹配
            if not cn_name:
                 std_t_by_name = next((t for t in std_data if t.get("name") == t_name or t.get("id") == t_name), None)
                 if std_t_by_name:
                     cn_name = std_t_by_name.get("name", "")
            
            label = f"{cn_name} ({t_name})" if cn_name else t_name
            options.append({"label": label, "value": t_name})
        
        return {
            "status": 0,
            "msg": "ok",
            "data": {
                "options": options
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询失败: {str(e)}")


@router.get("/table/{table_name}/columns")
async def get_table_columns(table_name: str):
    """
    获取指定表的所有列
    
    用于前端字段映射配置
    """
    try:
        importer = DataImporter()
        columns = importer.get_table_columns(table_name)
        
        # 转换为 AMIS 选项格式
        options = [{"label": c, "value": c} for c in columns]

        return {
            "status": 0,
            "msg": "ok",
            "data": {
                "table_name": table_name,
                "options": options
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询失败: {str(e)}")


@router.get("/suggest_mapping")
async def suggest_mapping(file_id: str, table_name: str):
    """
    获取推荐的字段映射
    
    Args:
        file_id: 上传文件的ID
        table_name: 目标表名
    """
    try:
        # 1. 获取文件列
        file_path = os.path.join(UPLOAD_DIR, file_id)
        if not os.path.exists(file_path):
             raise HTTPException(status_code=404, detail="文件不存在或已过期")
        
        importer = DataImporter()
        # 读取少量行以获取列头
        preview = importer.preview_file(file_path, max_rows=1)
        if not preview['success']:
             raise HTTPException(status_code=400, detail="文件读取失败")
        
        file_cols = preview['stats']['columns']
        
        # 2. 获取数据库表列
        db_cols = importer.get_table_columns(table_name)
        
        # 3. 进行匹配
        mapping = []
        
        # 预处理：转小写，去下划线
        def normalize(s):
            return s.lower().replace('_', '').replace(' ', '')
        
        # 建立数据库列的查找表
        db_col_map = {normalize(c): c for c in db_cols}
        
        # 遍历文件列寻找匹配
        for f_col in file_cols:
            norm_f = normalize(f_col)
            
            # 策略1: 精确匹配 (Normalized)
            if norm_f in db_col_map:
                mapping.append({
                    "source": f_col,
                    "target": db_col_map[norm_f]
                })
                continue
            
            # 策略2: 包含匹配 (可选，比较宽松)
            # 例如 "单位名称" vs "DWMC" (中文无法匹配英文简写，除非有元数据)
            # 这里如果有 schema 的中文名映射，可以增强匹配
            
            # 尝试利用 schema 里的中文名
            try:
                from hpf_audit.api.routes.schema import TABLE_ID_MAP, load_standard_schema
                std_data = load_standard_schema()
                
                # 找到目标表的标准定义
                cn_name = None
                std_id = TABLE_ID_MAP.get(table_name)
                target_std = None
                
                if std_id:
                     target_std = next((t for t in std_data if t['id'] == std_id), None)
                
                if not target_std:
                     target_std = next((t for t in std_data if t.get('name') == table_name or t.get('id') == table_name), None)
                
                if target_std:
                    for field in target_std.get('fields', []):
                        # 检查中文名是否匹配文件列
                        if field.get('name') == f_col or normalize(field.get('name', '')) == norm_f:
                             # 找到对应的代码 code
                             code = field.get('code')
                             # 确认该 code 存在于物理表中
                             if code in db_cols:
                                 mapping.append({
                                     "source": f_col,
                                     "target": code
                                 })
                                 break
            except ImportError:
                pass

        return {
            "status": 0,
            "msg": "ok",
            "data": mapping # 直接返回 mapping 数组，方便 AMIS 使用 setValue
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"匹配失败: {str(e)}")


@router.post("/prepare")
async def prepare_import(request: PrepareRequest):
    """
    准备导入 - 一站式获取文件列、数据库列和自动映射
    
    用于向导模式的第一步,返回所有需要的配置数据
    
    Args:
        request: 包含 file_id 和 table_name 的请求体
    
    Returns:
        {
            "file_columns": [...],  // 文件列选项
            "db_columns": [...],    // 数据库列选项
            "mapping": [...],       // 自动映射结果
            "file_info": {...}      // 文件基本信息
        }
    """
    try:
        file_id = request.file_id
        table_name = request.table_name
        
        # 1. 获取文件信息和列
        file_path = os.path.join(UPLOAD_DIR, file_id)
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="文件不存在或已过期")
        
        importer = DataImporter()
        
        # 预览文件获取列信息
        preview = importer.preview_file(file_path, max_rows=5)
        if not preview['success']:
            raise HTTPException(status_code=400, detail="文件读取失败")
        
        file_cols = preview['stats']['columns']
        file_info = {
            "file_id": file_id,
            "total_rows": preview['stats']['total_rows'],
            "columns": file_cols
        }
        
        # 2. 获取数据库表列
        db_cols = importer.get_table_columns(table_name)
        
        # 获取列的中文名映射(如果存在)
        try:
            std_data = load_standard_schema()
            std_id = TABLE_ID_MAP.get(table_name)
            target_std = None
            
            if std_id:
                target_std = next((t for t in std_data if t['id'] == std_id), None)
            
            if not target_std:
                target_std = next((t for t in std_data if t.get('name') == table_name or t.get('id') == table_name), None)
            
            # 构建列选项,包含中文名
            db_col_options = []
            for col in db_cols:
                label = col
                if target_std:
                    field = next((f for f in target_std.get('fields', []) if f.get('code') == col), None)
                    if field and field.get('name'):
                        label = f"{field['name']} ({col})"
                
                db_col_options.append({"label": label, "value": col})
        except:
            # 如果获取中文名失败,使用列名本身
            db_col_options = [{"label": c, "value": c} for c in db_cols]
        
        # 3. 执行自动匹配
        mapping = []
        
        def normalize(s):
            return s.lower().replace('_', '').replace(' ', '').replace('-', '')
        
        # 建立数据库列的查找表(normalized -> original)
        db_col_map = {normalize(c): c for c in db_cols}
        
        # 如果有中文名,也加入查找表
        if target_std:
            for field in target_std.get('fields', []):
                code = field.get('code')
                name = field.get('name')
                if code in db_cols and name:
                    db_col_map[normalize(name)] = code
        
        # 遍历文件列查找匹配
        for f_col in file_cols:
            norm_f = normalize(f_col)
            
            if norm_f in db_col_map:
                mapping.append({
                    "source": f_col,
                    "target": db_col_map[norm_f]
                })
        
        # 4. 构建文件列选项
        file_col_options = [{"label": c, "value": c} for c in file_cols]
        
        return {
            "status": 0,
            "msg": "准备成功",
            "data": {
                "file_columns": file_col_options,
                "db_columns": db_col_options,
                "mapping": mapping,
                "file_info": file_info
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"准备失败: {str(e)}")

