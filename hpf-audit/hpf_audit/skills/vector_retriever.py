"""
Vector Retriever using Chroma DB
基于 Chroma 向量库的语义检索器
"""
import os
import sqlite3
from typing import List, Dict, Any

try:
    import chromadb
    import chromadb
    from hpf_audit.skills.embedding_client import EmbeddingClient
    CHROMADB_AVAILABLE = True
except ImportError as e:
    CHROMADB_AVAILABLE = False
    print(f"⚠️  ChromaDB 不可用: {e}")

class VectorRetriever:
    """
    向量检索器 - 使用 Chroma DB 实现语义检索
    """
    def __init__(self, 
                 db_path: str = "./housing_provident_fund.db",
                 chroma_path: str = "./chroma_db"):
        """
        初始化向量检索器
        
        Args:
            db_path: SQLite 数据库路径（用于元数据）
            chroma_path: Chroma 向量库存储路径
        """
        if not CHROMADB_AVAILABLE:
            raise ImportError("ChromaDB 不可用，无法初始化向量检索器")
            
        self.db_path = os.path.abspath(db_path)
        self.chroma_path = chroma_path
        
        # 初始化 Chroma 客户端（持久化存储）
        self.client = chromadb.PersistentClient(path=chroma_path)
        
        # 获取或创建 collection
        try:
            self.collection = self.client.get_collection("knowledge_base")
            print(f"[VectorRetriever] 加载现有 collection，文档数: {self.collection.count()}")
        except:
            self.collection = self.client.create_collection(
                name="knowledge_base",
                metadata={"description": "公积金法规知识库"}
            )
            print("[VectorRetriever] 创建新 collection")
        
        # 初始化 Embedding 客户端（使用外部 API）
        print("[VectorRetriever] 初始化 Embedding 客户端...")
        self.model = EmbeddingClient()
        print("[VectorRetriever] Embedding 客户端初始化完成")

        # ✨ Self-Healing: 检查并修复数据库 Schema
        self._ensure_schema()

    def _ensure_schema(self):
        """确保数据库表结构正确 (自动迁移)"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 检查 META_KNOWLEDGE_BASE 是否有 metadata 列
            cursor.execute("PRAGMA table_info(META_KNOWLEDGE_BASE)")
            columns = [col[1] for col in cursor.fetchall()]
            
            if "metadata" not in columns:
                print("⚠️ [VectorRetriever] 检测到 'metadata' 列缺失，正在自动修复...")
                try:
                    cursor.execute("ALTER TABLE META_KNOWLEDGE_BASE ADD COLUMN metadata TEXT")
                    conn.commit()
                    print("✅ [VectorRetriever] 成功添加 'metadata' 列")
                except Exception as e:
                    print(f"❌ [VectorRetriever] 自动修复失败: {e}")
            
            conn.close()
        except Exception as e:
            print(f"⚠️ [VectorRetriever] Schema 检查失败: {e}")

    def add_knowledge(self, category: str, title: str, content: str, tags: List[str]):
        """
        向知识库添加条目（同时写入 SQLite 和 Chroma）
        
        Args:
            category: 类别（regulation, business_rule）
            title: 标题
            content: 正文内容
            tags: 标签列表
        """
        # 1. 写入 SQLite（元数据）
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        tags_str = ",".join(tags)
        cursor.execute(
            "INSERT INTO META_KNOWLEDGE_BASE (category, title, content, tags) VALUES (?, ?, ?, ?)",
            (category, title, content, tags_str)
        )
        knowledge_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        # 2. 生成向量并写入 Chroma
        # 将标题和内容拼接作为检索文本
        search_text = f"{title} {content}"
        embedding = self.model.encode(search_text)  # 已经是 list，不需要 .tolist()
        
        self.collection.add(
            ids=[f"kb_{knowledge_id}"],
            embeddings=[embedding],
            documents=[content],
            metadatas=[{
                "id": knowledge_id,
                "category": category,
                "title": title,
                "tags": tags_str
            }]
        )
        
        return knowledge_id

    def search(self, query: str, top_k: int = 3, filter: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        语义检索 (支持关联技能扩展)
        
        Args:
            query: 查询文本
            top_k: 返回结果数量
            filter: 过滤条件，例如 {"category": "skill_catalog"}
            
        Returns:
            检索结果列表，每项包含 id, category, title, content, tags, score, metadata
        """
        # ✨ 0. 检查 Chroma 是否为空，如果为空直接用数据库检索
        collection_count = self.collection.count()
        if collection_count == 0:
            print("⚠️ Chroma 向量索引为空，使用数据库全量检索")
            results = self._search_from_db_only(query, top_k, filter)
            return self._expand_related_skills(results) # 同样支持扩展
        
        # 1. 生成查询向量
        query_embedding = self.model.encode(query)  # 已经是 list，不需要 .tolist()
        
        # 2. 向量检索
        results = self.collection.query(
            query_embeddings=[query_embedding],
            n_results=top_k,
            where=filter  # 添加过滤条件
        )
        
        # 3. 格式化返回结果
        formatted_results = []
        
        if results['ids'] and len(results['ids'][0]) > 0:
            for i in range(len(results['ids'][0])):
                metadata = results['metadatas'][0][i]
                document = results['documents'][0][i]
                distance = results['distances'][0][i] if 'distances' in results else 0
                
                # 解析 metadata 字符串 (如果存储时是 JSON 字符串)
                meta_dict = metadata.get("metadata", "{}")
                if isinstance(meta_dict, str):
                    try:
                        import json
                        meta_dict = json.loads(meta_dict)
                    except:
                        meta_dict = {}
                else:
                    meta_dict = meta_dict if meta_dict else {}

                formatted_results.append({
                    "id": metadata.get("id"),
                    "category": metadata.get("category"),
                    "title": metadata.get("title"),
                    "content": document,
                    "tags": metadata.get("tags"),
                    "score": max(0, 1 - distance),  # 距离转相似度，确保非负
                    "metadata": meta_dict
                })
        
        # 4. ✨ 补充数据库中的 skill_catalog 记录（Chroma 中可能还没有）
        # (保持原有的补充逻辑，简化版)
        self._supplement_from_db(query, formatted_results, top_k)
        
        # 5. 按分数排序
        formatted_results.sort(key=lambda x: x['score'], reverse=True)
        primary_results = formatted_results[:top_k * 2]
        
        # 6. ✨ 关联技能扩展 (Skill Graph Expansion)
        final_results = self._expand_related_skills(primary_results)
        
        return final_results

    def _supplement_from_db(self, query: str, formatted_results: List[Dict], top_k: int):
        """补充数据库中的记录（未在 Chroma 中的）"""
        try:
            import sqlite3
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 查询所有 skill_catalog 记录
            cursor.execute("""
                SELECT id, category, title, content, tags, metadata
                FROM META_KNOWLEDGE_BASE 
                WHERE category = 'skill_catalog'
            """)
            
            skill_records = cursor.fetchall()
            conn.close()
            
            # 将数据库 skill 加入结果（如果还不在 Chroma 结果中）
            existing_ids = {str(r['id']) for r in formatted_results} # ensure string comparison if needed
            
            query_lower = query.lower()
            query_words = set(query_lower.split())

            for row in skill_records:
                skill_id = str(row[0])
                if skill_id not in existing_ids:
                    content = row[3]
                    title = row[2]
                    
                    # 简单的文本相似度
                    text_words = set((content + " " + title).lower().split())
                    overlap = len(query_words & text_words)
                    
                    if overlap > 0:
                        sim_score = min(overlap / max(len(query_words), 1) * 0.8, 0.8)
                        
                        # 解析 metadata
                        import json
                        try:
                            meta_dict = json.loads(row[5]) if row[5] else {}
                        except:
                            meta_dict = {}

                        formatted_results.append({
                            "id": row[0],
                            "category": row[1],
                            "title": row[2],
                            "content": row[3],
                            "tags": row[4],
                            "score": sim_score,
                            "metadata": meta_dict
                        })
        except Exception as e:
            print(f"⚠️ 补充 skill_catalog 失败: {e}")

    def _expand_related_skills(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        图谱扩展：查找结果中关联的技能
        """
        if not results:
            return []
            
        related_skill_ids = set()
        existing_skill_ids = set()
        
        # 收集当前结果集中的 skill_id 和引用的 related_skills
        for res in results:
            meta = res.get("metadata", {})
            if not meta: continue
            
            # 记录当前已存在的 skill_id
            sid = meta.get("skill_id")
            if sid:
                existing_skill_ids.add(sid)
            
            # 收集关联的 skill_ids
            rels = meta.get("related_skills", [])
            if rels:
                for rid in rels:
                    related_skill_ids.add(rid)
        
        # 移除已经存在于结果中的
        related_skill_ids = related_skill_ids - existing_skill_ids
        
        if not related_skill_ids:
            return results
            
        # 从数据库批量获取关联技能
        print(f"[VectorRetriever] 发现关联技能引用: {related_skill_ids}，正在扩展...")
        
        try:
            import sqlite3
            import json
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            placeholders = ','.join(['?'] * len(related_skill_ids))
            # 注意：这里我们查的是 META_KNOWLEDGE_BASE 通过 metadata 里的 skill_id 匹配
            # 为了性能，也可以直接查 META_SKILL_DEF，但为了保持返回格式一致，查 META_KNOWLEDGE_BASE 更好
            
            sql = f"""
                SELECT id, category, title, content, tags, metadata
                FROM META_KNOWLEDGE_BASE 
                WHERE category = 'skill_catalog' 
                AND metadata LIKE ?  -- 这是一个简化的模糊匹配，不够严谨但对于 JSON 字符串查询暂时可用
                -- 更严谨的做法是遍历或者使用 JSON 扩展，或者直接根据 skill_id 查 META_SKILL_DEF 并构造
            """
            
            # 由于 SQLite LIKE 只能匹配一个，我们改用 Python 过滤或多次查询
            # 方案 B: 直接查 META_SKILL_DEF，因为它是源头
            sql_defs = f"""
                SELECT skill_id, name, description, markdown_content, configuration
                FROM META_SKILL_DEF
                WHERE skill_id IN ({placeholders})
            """
            
            cursor.execute(sql_defs, list(related_skill_ids))
            rows = cursor.fetchall()
            conn.close()
            
            expanded_results = []
            for row in rows:
                sid, name, desc, content, config_str = row
                
                # 解析配置以获取 metadata 用于一致性
                try:
                    import yaml
                    config = yaml.safe_load(config_str)
                    # 构造类似 search 结果的结构
                    # Score 设为 0.0 (或特数值) 表示它是关联出来的，不是搜出来的
                    
                    # 构造 metadata
                    meta_dict = {
                        "skill_id": sid,
                        "db_table": "META_SKILL_DEF",
                        "skill_type": config.get('template_type'),
                        "parameters": {
                            p['name']: {
                                "type": p.get('type', 'string'),
                                "default": p.get('default'),
                                "required": p.get('required', False)
                            } for p in config.get('parameters', [])
                        },
                        "related_skills": config.get('meta', {}).get('related_skills', []),
                        "is_related": True # ✨ 标记为关联技能
                    }
                    
                    expanded_results.append({
                        "id": f"db_{sid}", # 虚拟ID
                        "category": "skill_catalog",
                        "title": name,
                        "content": f"名称：{name}\n功能：{desc}\n(关联推荐)",
                        "tags": "",
                        "score": 0.0, # 关联技能不参与向量排序，但在展示时可以用特殊逻辑
                        "metadata": meta_dict
                    })
                except Exception as e:
                    print(f"⚠️ 解析关联技能 {sid} 失败: {e}")
            
            if expanded_results:
                print(f"[VectorRetriever] 成功扩展 {len(expanded_results)} 个关联技能")
                # 将关联技能追加到结果后面
                results.extend(expanded_results)
                
        except Exception as e:
            print(f"⚠️ 关联技能查询失败: {e}")
            
        return results

    def _search_from_db_only(self, query: str, top_k: int = 10, filter: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        仅从数据库检索（Chroma 为空时的后备方案）
        """
        import sqlite3
        import json
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 构建查询
        sql = "SELECT id, category, title, content, tags, metadata FROM META_KNOWLEDGE_BASE"
        params = []
        
        if filter and "category" in filter:
            sql += " WHERE category = ?"
            params.append(filter["category"])
            
        cursor.execute(sql, params)
        all_records = cursor.fetchall()
        conn.close()
        
        # 计算相似度
        results = []
        query_lower = query.lower()
        query_words = set(query_lower.split())
        
        for row in all_records:
            try:
                meta_dict = json.loads(row[5]) if row[5] else {}
            except:
                meta_dict = {}
                
            title = row[2]
            content = row[3]
            
            # 关键词匹配
            text = (title + " " + content).lower()
            
            # 简单的子串匹配（适应中文）
            overlap = 0
            for qw in query_words:
                if qw in text:
                    overlap += 1
            
            if overlap > 0:
                sim_score = overlap / len(query_words)
                
                results.append({
                    "id": row[0],
                    "category": row[1],
                    "title": row[2],
                    "content": row[3],
                    "tags": row[4],
                    "score": min(sim_score, 0.9),
                    "metadata": meta_dict
                })
        
        # 排序并返回
        results.sort(key=lambda x: x['score'], reverse=True)
        return results[:top_k]

    def rebuild_index(self):
        """
        从 SQLite 重建向量索引
        清空 Chroma 并重新导入所有数据
        """
        print("[VectorRetriever] 开始重建索引...")
        
        # 1. 清空现有 collection
        self.client.delete_collection("knowledge_base")
        self.collection = self.client.create_collection(
            name="knowledge_base",
            metadata={"description": "公积金法规知识库"}
        )
        
        # 2. 从 SQLite 读取所有数据
        conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM META_KNOWLEDGE_BASE")
        rows = cursor.fetchall()
        conn.close()
        
        # 3. 批量向量化并写入 Chroma
        if rows:
            ids = []
            embeddings = []
            documents = []
            metadatas = []
            
            for row in rows:
                row_dict = dict(row)
                knowledge_id = row_dict['id']
                
                # 生成检索文本和向量
                search_text = f"{row_dict['title']} {row_dict['content']}"
                embedding = self.model.encode(search_text)  # 已经是 list，不需要 .tolist()
                
                ids.append(f"kb_{knowledge_id}")
                embeddings.append(embedding)
                documents.append(row_dict['content'])
                metadatas.append({
                    "id": knowledge_id,
                    "category": row_dict['category'],
                    "title": row_dict['title'],
                    "tags": row_dict.get('tags', '')
                })
            
            # 批量添加
            self.collection.add(
                ids=ids,
                embeddings=embeddings,
                documents=documents,
                metadatas=metadatas
            )
            
            print(f"[VectorRetriever] 索引重建完成，导入 {len(ids)} 条文档")
        else:
            print("[VectorRetriever] 数据库为空，无需重建索引")

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            "total_documents": self.collection.count(),
            "chroma_path": self.chroma_path,
            "provider": self.model.provider,
            "model": self.model.model
        }
