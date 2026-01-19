"""
Skill 管理 API 路由
提供 Skill 的 CRUD、测试、发布等功能
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List
import yaml
import json
from datetime import datetime

# ✅ 作为hpf_audit子模块
from hpf_common.db import DBManager
from hpf_common.config import settings
from hpf_audit.knowledge.vector_store import VectorStoreManager
import sqlite3

def get_db_connection():
    conn = sqlite3.connect(settings.audit_db_path)
    conn.row_factory = sqlite3.Row
    return conn

router = APIRouter(prefix="/api/skills", tags=["skills"])


class SkillUpdateRequest(BaseModel):
    """更新 Skill 请求"""
    name: Optional[str] = None
    description: Optional[str] = None
    configuration: Optional[str] = None  # YAML 配置
    is_active: Optional[int] = None


class SkillTestRequest(BaseModel):
    """测试 Skill 请求"""
    skill_id: str
    params: Optional[dict] = {}


class SkillTestResponse(BaseModel):
    """测试结果响应"""
    success: bool
    skill_id: str
    result: Optional[dict] = None
    error: Optional[str] = None
    execution_time_ms: Optional[float] = None


@router.get("/list")
async def list_skills(
    status: Optional[str] = None,  # all, active, shadow
    page: int = 1,
    page_size: int = 20
):
    """
    获取 Skill 列表
    
    - status: all(全部), active(已激活), shadow(影子模式)
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 构建查询条件
    where_clause = "WHERE 1=1"
    params = []
    
    if status == "active":
        where_clause += " AND is_active = 1"
    elif status == "shadow":
        where_clause += " AND is_active = 0"
    
    # 获取总数
    cursor.execute(f"SELECT COUNT(*) FROM META_SKILL_DEF {where_clause}", params)
    total = cursor.fetchone()[0]
    
    # 分页查询
    offset = (page - 1) * page_size
    cursor.execute(f"""
        SELECT skill_id, name, description, template_type, is_active, 
               created_at, updated_at, configuration
        FROM META_SKILL_DEF 
        {where_clause}
        ORDER BY updated_at DESC
        LIMIT ? OFFSET ?
    """, params + [page_size, offset])
    
    rows = cursor.fetchall()
    conn.close()
    
    skills = []
    for row in rows:
        skill = dict(row)
        # 解析配置中的 tags
        if skill.get('configuration'):
            try:
                config = yaml.safe_load(skill['configuration'])
                skill['tags'] = config.get('meta', {}).get('tags', [])
            except:
                skill['tags'] = []
        else:
            skill['tags'] = []
        skills.append(skill)
    
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "skills": skills
    }


@router.get("/detail/{skill_id}")
async def get_skill_detail(skill_id: str):
    """获取 Skill 详情"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT * FROM META_SKILL_DEF WHERE skill_id = ?
    """, (skill_id,))
    
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        raise HTTPException(status_code=404, detail=f"Skill '{skill_id}' 不存在")
    
    skill = dict(row)
    
    # 解析 YAML 配置
    if skill.get('configuration'):
        try:
            skill['config_parsed'] = yaml.safe_load(skill['configuration'])
        except Exception as e:
            skill['config_parsed'] = None
            skill['config_error'] = str(e)
    
    return skill


# @router.post("/generate")
# async def generate_skill(request: SkillCreateRequest):
#     """
#     AI 生成 Skill 配置 - 已废弃，使用 /api/agent/generate_skill 代替
#     """
#     return {
#         "success": False,
#         "error": "此接口已废弃，请使用 /api/agent/generate_skill"
#     }


@router.post("/save_manual")
async def save_manual_skill(request: dict):
    """
    保存手动配置的 Skill
    """
    try:
        skill_id = request.get('skill_id')
        configuration = request.get('configuration')
        is_active = request.get('is_active', 0)
        name = request.get('name', '未命名 Skill')
        description = request.get('description', '')
        
        if not skill_id or not configuration:
            return {
                "success": False,
                "error": "缺少必要参数: skill_id 或 configuration"
            }
        
        # 验证 YAML 格式
        try:
            import yaml
            config_dict = yaml.safe_load(configuration)
        except Exception as e:
            return {
                "success": False,
                "error": f"YAML 格式错误: {str(e)}"
            }
        
        # 保存到数据库
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 检查是否已存在
        cursor.execute("SELECT skill_id FROM META_SKILL_DEF WHERE skill_id = ?", (skill_id,))
        exists = cursor.fetchone()
        
        if exists:
            # 更新
            cursor.execute("""
                UPDATE META_SKILL_DEF 
                SET name = ?, description = ?, configuration = ?, is_active = ?, updated_at = CURRENT_TIMESTAMP
                WHERE skill_id = ?
            """, (name, description, configuration, is_active, skill_id))
        else:
            # 插入
            cursor.execute("""
                INSERT INTO META_SKILL_DEF 
                (skill_id, name, description, configuration, template_type, is_active)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (skill_id, name, description, configuration, 'sql_risk_check', is_active))
        
        conn.commit()
        conn.close()
        
        return {
            "success": True,
            "skill_id": skill_id,
            "message": "Skill 保存成功"
        }
    
    except Exception as e:
        import traceback
        return {
            "success": False,
            "error": f"保存失败: {str(e)}",
            "traceback": traceback.format_exc()
        }
    finally:
        #此处尝试同步向量库
        try:
            if 'skill_id' in locals() and 'name' in locals() and 'description' in locals():
                vsm = VectorStoreManager()
                vsm.add_skills([{
                    "skill_id": skill_id,
                    "name": name,
                    "description": description
                }])
        except Exception as e:
            print(f"⚠️ 向量库同步失败: {e}")


@router.put("/update/{skill_id}")
async def update_skill(skill_id: str, request: SkillUpdateRequest):
    """更新 Skill 配置"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 检查是否存在
    cursor.execute("SELECT skill_id FROM META_SKILL_DEF WHERE skill_id = ?", (skill_id,))
    if not cursor.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail=f"Skill '{skill_id}' 不存在")
    
    # 构建更新语句
    updates = []
    params = []
    
    if request.name is not None:
        updates.append("name = ?")
        params.append(request.name)
    
    if request.description is not None:
        updates.append("description = ?")
        params.append(request.description)
    
    if request.configuration is not None:
        # 验证 YAML 格式
        try:
            yaml.safe_load(request.configuration)
        except Exception as e:
            conn.close()
            raise HTTPException(status_code=400, detail=f"YAML 格式错误: {str(e)}")
        
        updates.append("configuration = ?")
        params.append(request.configuration)
    
    if request.is_active is not None:
        updates.append("is_active = ?")
        params.append(request.is_active)
    
    if not updates:
        conn.close()
        return {"success": True, "message": "无更新内容"}
    
    updates.append("updated_at = CURRENT_TIMESTAMP")
    params.append(skill_id)
    
    sql = f"UPDATE META_SKILL_DEF SET {', '.join(updates)} WHERE skill_id = ?"
    cursor.execute(sql, params)
    conn.commit()
    
    # ✨ 新增：同步更新向量库
    # ✨ 新增：同步更新向量库
    try:
        # 获取更新后的完整配置
        cursor = conn.cursor()
        cursor.execute("SELECT configuration, name, description FROM META_SKILL_DEF WHERE skill_id = ?", (skill_id,))
        row = cursor.fetchone()
        
        if row:
            vsm = VectorStoreManager()
            # 由于可能修改了name/desc，需要重新索引
            # FAISS add 实际上是 append，最好先删除旧的? 
            # VectorStoreManager add_skills 使用skill_id作为ID，如果ID相同会覆盖文档内容(docstore中)，
            # 但Index中可能残留旧向量。最安全是先删后加。
            vsm.delete_skill(skill_id)
            vsm.add_skills([{
                "skill_id": skill_id,
                "name": row['name'],
                "description": row['description'] or ""
            }])
            
            print(f"✅ Skill '{skill_id}' 向量库已同步更新")
    except Exception as e:
        print(f"⚠️ 向量库同步失败（不影响主流程）: {e}")
    
    conn.close()
    
    return {"success": True, "skill_id": skill_id}


@router.post("/test")
async def test_skill(request: SkillTestRequest):
    """
    测试 Skill 执行
    
    在不影响生产数据的情况下测试 Skill
    """
    import time
    start_time = time.time()
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 获取 Skill 配置
    cursor.execute("""
        SELECT configuration, template_type FROM META_SKILL_DEF WHERE skill_id = ?
    """, (request.skill_id,))
    
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        return SkillTestResponse(
            success=False,
            skill_id=request.skill_id,
            error=f"Skill '{request.skill_id}' 不存在"
        )
    
    config_yaml = row['configuration']
    if not config_yaml:
        return SkillTestResponse(
            success=False,
            skill_id=request.skill_id,
            error="Skill 配置为空"
        )
    
    try:
        # 解析配置
        config_dict = yaml.safe_load(config_yaml)
        
        # 使用 SkillFactory 创建实例
        from hpf_audit.skills.template_engine import SkillFactory
        skill_instance = SkillFactory.create_skill(config_dict)
        
        # 执行测试
        result = skill_instance.execute(**request.params)
        
        execution_time = (time.time() - start_time) * 1000
        
        return SkillTestResponse(
            success=True,
            skill_id=request.skill_id,
            result=result,
            execution_time_ms=round(execution_time, 2)
        )
    
    except Exception as e:
        import traceback
        return SkillTestResponse(
            success=False,
            skill_id=request.skill_id,
            error=str(e),
            result={"traceback": traceback.format_exc()}
        )


@router.post("/publish/{skill_id}")
async def publish_skill(skill_id: str):
    """
    发布 Skill（从 Shadow Mode 切换到 Active）
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 检查是否存在
    cursor.execute("SELECT is_active FROM META_SKILL_DEF WHERE skill_id = ?", (skill_id,))
    row = cursor.fetchone()
    
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail=f"Skill '{skill_id}' 不存在")
    
    if row['is_active'] == 1:
        conn.close()
        return {"success": True, "message": "Skill 已经是激活状态"}
    
    # 激活
    cursor.execute("""
        UPDATE META_SKILL_DEF 
        SET is_active = 1, updated_at = CURRENT_TIMESTAMP 
        WHERE skill_id = ?
    """, (skill_id,))
    conn.commit()
    conn.close()
    
    return {"success": True, "skill_id": skill_id, "message": "Skill 已发布激活"}


@router.post("/unpublish/{skill_id}")
async def unpublish_skill(skill_id: str):
    """
    下线 Skill（从 Active 切换到 Shadow Mode）
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        UPDATE META_SKILL_DEF 
        SET is_active = 0, updated_at = CURRENT_TIMESTAMP 
        WHERE skill_id = ?
    """, (skill_id,))
    
    if cursor.rowcount == 0:
        conn.close()
        raise HTTPException(status_code=404, detail=f"Skill '{skill_id}' 不存在")
    
    conn.commit()
    conn.close()
    
    return {"success": True, "skill_id": skill_id, "message": "Skill 已下线"}


@router.delete("/delete/{skill_id}")
async def delete_skill(skill_id: str):
    """删除 Skill"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("DELETE FROM META_SKILL_DEF WHERE skill_id = ?", (skill_id,))
    
    if cursor.rowcount == 0:
        conn.close()
        raise HTTPException(status_code=404, detail=f"Skill '{skill_id}' 不存在")
    
    conn.commit()
    
    # ✨ 新增：同步删除向量库中的记录
    # ✨ 新增：同步删除向量库中的记录
    try:
        vsm = VectorStoreManager()
        success = vsm.delete_skill(skill_id)
        if success:
            print(f"✅ Skill '{skill_id}' 从知识库删除成功")
    except Exception as e:
        print(f"⚠️ 知识库删除失败（不影响主流程）: {e}")
    
    conn.close()
    
    return {"success": True, "skill_id": skill_id, "message": "Skill 已删除"}


@router.post("/validate")
async def validate_skill_config(config: dict):
    """
    验证 Skill 配置格式
    
    请求体直接传入 YAML 解析后的配置对象
    """
    try:
        from hpf_audit.skills.validator import ConfigurationValidator
        
        # 转回 YAML 字符串进行验证
        yaml_str = yaml.dump(config, allow_unicode=True)
        
        validator = ConfigurationValidator()
        is_valid, error, parsed = validator.validate_yaml(yaml_str)
        
        if is_valid:
            return {
                "valid": True,
                "message": "配置格式正确",
                "parsed": parsed
            }
        else:
            return {
                "valid": False,
                "error": error
            }
    
    except Exception as e:
        return {
            "valid": False,
            "error": f"验证失败: {str(e)}"
        }


@router.get("/templates")
async def get_skill_templates():
    """获取可用的 Skill 模板类型"""
    return {
        "templates": [
            {
                "type": "sql_risk_check",
                "name": "SQL 风险检查",
                "description": "执行 SQL 查询并根据风险逻辑判断结果",
                "example": """skill_id: example_risk_check
template_type: sql_risk_check
meta:
  name: 示例风险检查
  description: 检查某类风险
  tags:
    - 风险
    - 示例

sql_template: |
  SELECT * FROM t_loan_apply
  WHERE loan_status = '逾期'
  LIMIT {{ limit }}

parameters:
  - name: limit
    type: integer
    description: 返回记录数量
    default: 100
    required: false

risk_logic:
  risk_level: High
  condition: "len(results) > 0"
  message: "发现 {len(results)} 条风险记录"
"""
            }
        ]
    }
