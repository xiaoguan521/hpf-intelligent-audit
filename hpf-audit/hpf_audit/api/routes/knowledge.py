"""
Knowledge Base Management API
提供知识库的增删改查接口
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator
from typing import List, Optional, Union
import sqlite3
from hpf_audit.api.database import get_db_connection, DB_PATH
from hpf_audit.knowledge.vector_store import VectorStoreManager

router = APIRouter(prefix="/api/knowledge", tags=["Knowledge Base"])

def _ensure_knowledge_table():
    """Ensure the knowledge base table exists"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS META_KNOWLEDGE_BASE (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT NOT NULL,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                tags TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error creating META_KNOWLEDGE_BASE table: {e}")

# Initialize table on import
_ensure_knowledge_table()

class KnowledgeItem(BaseModel):
    """知识条目模型"""
    category: str  # regulation, business_rule, case_study
    title: str
    content: str
    tags: Union[str, List[str]] = ""  # 支持字符串或列表格式
    
    @field_validator('tags', mode='before')
    @classmethod
    def convert_tags(cls, v):
        """将字符串格式的 tags 转换为列表"""
        if isinstance(v, str):
            return [t.strip() for t in v.split(',') if t.strip()] if v else []
        return v

class KnowledgeResponse(BaseModel):
    """知识条目响应"""
    id: int
    category: str
    title: str
    content: str
    tags: str
    created_at: str

@router.post("/add")
async def add_knowledge(item: KnowledgeItem):
    """
    添加新的知识条目
    
    示例:
    POST /api/knowledge/add
    {
        "category": "regulation",
        "title": "离退休提取政策",
        "content": "职工离休、退休的，可以提取...",
        "tags": ["离退休", "提取"]
    }
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        tags_str = ",".join(item.tags) if isinstance(item.tags, list) else item.tags
        
        cursor.execute("""
            INSERT INTO META_KNOWLEDGE_BASE (category, title, content, tags)
            VALUES (?, ?, ?, ?)
        """, (item.category, item.title, item.content, tags_str))
        
        knowledge_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        # ✨ 同步添加到 FAISS VectorStore
        try:
            vsm = VectorStoreManager()
            vsm.add_knowledge([{
                "id": knowledge_id,
                "category": item.category,
                "title": item.title,
                "content": item.content,
                "tags": tags_str
            }])
        except Exception as e:
            print(f"⚠️ VectorStore 添加失败（不影响主流程）: {e}") 
        
        return {
            "status": 0,
            "msg": "添加成功",
            "data": {"id": knowledge_id}
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/list")
async def list_knowledge(
    category: Optional[str] = None,
    limit: int = 50
):
    """
    查询知识库列表
    
    示例:
    GET /api/knowledge/list?category=regulation&limit=10
    """
    try:
        conn = get_db_connection(readonly=True)
        cursor = conn.cursor()
        
        if category:
            cursor.execute("""
                SELECT * FROM META_KNOWLEDGE_BASE 
                WHERE category = ? 
                ORDER BY created_at DESC 
                LIMIT ?
            """, (category, limit))
        else:
            cursor.execute("""
                SELECT * FROM META_KNOWLEDGE_BASE 
                ORDER BY created_at DESC 
                LIMIT ?
            """, (limit,))
        
        rows = cursor.fetchall()
        conn.close()
        
        return {
            "status": 0,
            "msg": "ok",
            "data": {
                "items": [dict(row) for row in rows],
                "total": len(rows)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/{knowledge_id}")
async def delete_knowledge(knowledge_id: int):
    """
    删除知识条目
    
    示例:
    DELETE /api/knowledge/5
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM META_KNOWLEDGE_BASE WHERE id = ?", (knowledge_id,))
        
        if cursor.rowcount == 0:
            conn.close()
            raise HTTPException(status_code=404, detail="知识条目不存在")
        
        conn.commit()
        conn.close()
        
        # ✨ 同步删除 FAISS VectorStore 中的向量
        try:
            vsm = VectorStoreManager()
            # 知识库文档ID格式: kb_{id}
            vsm.delete_document(f"kb_{knowledge_id}")
        except Exception as e:
            print(f"⚠️ VectorStore 删除失败（不影响主流程）: {e}")
        
        return {
            "status": 0,
            "msg": "删除成功"
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/search")
async def search_knowledge(q: str, limit: int = 10):
    """
    搜索知识库
    
    示例:
    GET /api/knowledge/search?q=购房&limit=5
    """
    try:
        conn = get_db_connection(readonly=True)
        cursor = conn.cursor()
        
        search_pattern = f"%{q}%"
        cursor.execute("""
            SELECT * FROM META_KNOWLEDGE_BASE 
            WHERE title LIKE ? OR content LIKE ? OR tags LIKE ?
            ORDER BY created_at DESC
            LIMIT ?
        """, (search_pattern, search_pattern, search_pattern, limit))
        
        rows = cursor.fetchall()
        conn.close()
        
        return {
            "status": 0,
            "msg": "ok",
            "data": {
                "items": [dict(row) for row in rows],
                "total": len(rows)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
