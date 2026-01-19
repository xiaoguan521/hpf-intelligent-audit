import sqlite3
import json
from typing import List, Dict, Any
from hpf_audit.skills.mcp_client import MCPClient

class SimpleRetriever:
    """
    A lightweight retriever compatible with SQLite.
    Uses keyword matching (LIKE) + LLM Re-ranking (Conceptual).
    """
    def __init__(self, db_path: str = "./housing_provident_fund.db"):
        self.db_path = db_path

    def search(self, query: str, top_k: int = 3) -> List[Dict[str, Any]]:
        """
        Search for relevant knowledge chunks.
        current implementation: Simple keyword matching based on splitting the query.
        """
        # 1. Extract Keywords (Simple heuristics for now)
        # In production, use LLM to extract "search terms"
        keywords = [w for w in query.split() if len(w) > 1 and w not in ["查询", "查找", "分析", "审计", "检查"]]
        keywords = list(set(keywords))
        
        if not keywords:
            return []
            
        conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        results = []
        try:
            # Dynamically build SQL for partial match
            conditions = []
            params = []
            for kw in keywords:
                conditions.append("(title LIKE ? OR tags LIKE ? OR content LIKE ?)")
                p = f"%{kw}%"
                params.extend([p, p, p])
            
            where_clause = " OR ".join(conditions)
            sql = f"""
            SELECT id, category, title, content, tags 
            FROM META_KNOWLEDGE_BASE 
            WHERE {where_clause}
            LIMIT ?
            """
            
            cursor.execute(sql, params + [top_k * 2]) # Fetch more candidates
            rows = cursor.fetchall()
            
            # Simple Scoring: Count keyword hits
            scored_rows = []
            for row in rows:
                score = 0
                text = (row['title'] + row['tags'] + row['content']).lower()
                for kw in keywords:
                    if kw.lower() in text:
                        score += 1
                scored_rows.append({"doc": dict(row), "score": score})
            
            # Sort by score
            scored_rows.sort(key=lambda x: x["score"], reverse=True)
            results = [x["doc"] for x in scored_rows[:top_k]]
            
        except Exception as e:
            print(f"Retrieval Error: {e}")
            pass
        finally:
            conn.close()
            
        return results

    def add_knowledge(self, category: str, title: str, content: str, tags: List[str]):
        """Ingest knowledge into DB"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        tags_str = ",".join(tags)
        cursor.execute(
            "INSERT INTO META_KNOWLEDGE_BASE (category, title, content, tags) VALUES (?, ?, ?, ?)",
            (category, title, content, tags_str)
        )
        conn.commit()
        conn.close()
