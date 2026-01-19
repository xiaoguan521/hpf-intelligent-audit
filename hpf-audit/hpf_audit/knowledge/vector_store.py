"""
向量存储管理器 (基于LangChain + FAISS)
使用API Embedding，不下载本地模型
"""
from typing import List, Dict, Optional
from pathlib import Path
import pickle
import numpy as np

try:
    from langchain_community.vectorstores import FAISS
    from langchain_core.documents import Document
    from langchain_core.embeddings import Embeddings
    LANGCHAIN_AVAILABLE = True
except ImportError:
    pass
    LANGCHAIN_AVAILABLE = False
    print("Warning: LangChain not available, vector store disabled")


class APIEmbeddings(Embeddings):
    """使用hpf_common的API Embedding"""
    
    def __init__(self):
        from hpf_common.embedding import EmbeddingClient
        self.client = EmbeddingClient()
    
    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        """嵌入多个文档"""
        # 直接传递列表给client (client已处理批量)
        # client.embed 返回 List[List[float]]
        return self.client.embed(texts)
    
    def embed_query(self, text: str) -> List[float]:
        """嵌入单个查询"""
        # 取第一条结果
        result = self.client.embed(text)
        return result[0] if result else []


class VectorStoreManager:
    """统一的向量存储管理器"""
    
    def __init__(
        self, 
        index_path: str = "data/faiss_index"
    ):
        if not LANGCHAIN_AVAILABLE:
            raise ImportError("LangChain is required. Run: pip install langchain langchain-community faiss-cpu")
        
        self.index_path = Path(index_path)
        self.index_path.mkdir(parents=True, exist_ok=True)
        
        # ✅ 使用API Embedding (不下载模型)
        print(f"📡 使用API Embedding (不下载本地模型)")
        self.embeddings = APIEmbeddings()
        
        # 加载或创建向量存储
        self.vectorstore = self._load_or_create()
        print(f"✅ 向量存储已就绪: {self.index_path}")
    
    def _load_or_create(self) -> FAISS:
        """加载现有索引或创建新索引"""
        index_file = self.index_path / "index.faiss"
        
        if index_file.exists():
            try:
                return FAISS.load_local(
                    str(self.index_path),
                    self.embeddings,
                    allow_dangerous_deserialization=True
                )
            except Exception as e:
                print(f"⚠️ 加载索引失败: {e}, 创建新索引")
        
        # 创建空索引
        return FAISS.from_documents(
            [Document(page_content="初始化文档", metadata={"type": "init"})],
            self.embeddings
        )
    
    def add_skills(self, skills: List[Dict]):
        """
        添加Skills到向量库
        
        Args:
            skills: [
                {
                    "skill_id": "withdrawal_audit",
                    "name": "提取审计",
                    "description": "检查提取业务异常...",
                }
            ]
        """
        documents = []
        for skill in skills:
            # 构建检索文本 (优先使用传入的 content)
            if "content" in skill and skill["content"]:
                content = skill["content"]
            else:
                content = f"""
技能名称: {skill['name']}
技能ID: {skill['skill_id']}
功能描述: {skill['description']}
                """.strip()
            
            metadata = {
                "skill_id": skill["skill_id"],
                "name": skill["name"],
                "type": "skill"
            }
            # 合并自定义元数据
            if "metadata" in skill:
                metadata.update(skill["metadata"])

            doc = Document(
                page_content=content,
                metadata=metadata
            )
            documents.append(doc)
        
        ids = [skill["skill_id"] for skill in skills]
        
        # 添加到向量库 (使用skill_id作为文档ID)
        self.vectorstore.add_documents(documents, ids=ids)
        self.save()
        print(f"✅ 已索引 {len(skills)} 个Skills")

    def add_knowledge(self, items: List[Dict]):
        """
        添加通用知识到向量库
        
        Args:
            items: [
                {
                    "id": 1,
                    "title": "...",
                    "content": "...",
                    "category": "regulation",
                    "tags": "a,b"
                }
            ]
        """
        if not items: return
        
        documents = []
        ids = []
        for item in items:
            content = f"{item['title']}\n{item['content']}"
            doc_id = f"kb_{item['id']}"
            
            doc = Document(
                page_content=content,
                metadata={
                    "id": item["id"],
                    "title": item["title"],
                    "category": item["category"],
                    "tags": item.get("tags", ""),
                    "type": "knowledge"
                }
            )
            documents.append(doc)
            ids.append(doc_id)
            
        self.vectorstore.add_documents(documents, ids=ids)
        self.save()
        print(f"✅ 已索引 {len(items)} 条知识")

    def delete_document(self, doc_id: str):
        """从向量库删除文档 (通用)"""
        try:
            self.vectorstore.delete([doc_id])
            self.save()
            print(f"✅ 已从向量库删除: {doc_id}")
            return True
        except Exception as e:
            print(f"⚠️ 从向量库删除失败: {e}")
            return False

    def delete_skill(self, skill_id: str):
        """兼容旧接口: 删除 Skill"""
        return self.delete_document(skill_id)

    def search(
        self, 
        query: str, 
        top_k: int = 3,
        filter_dict: Optional[Dict] = None
    ) -> List[Dict]:
        """
        通用语义搜索
        """
        docs_with_scores = self.vectorstore.similarity_search_with_score(
            query,
            k=top_k,
            filter=filter_dict
        )
        
        results = []
        for doc, score in docs_with_scores:
            if doc.metadata.get("type") == "init": continue
                
            results.append({
                "content": doc.page_content,
                "score": float(1 - score),
                "metadata": doc.metadata
            })
        return results
    
    def search_skills(
        self, 
        query: str, 
        top_k: int = 3,
        filter_dict: Optional[Dict] = None
    ) -> List[Dict]:
        """
        语义搜索Skills
        
        Returns:
            [
                {
                    "skill_id": "...",
                    "name": "...",
                    "score": 0.85,
                    "content": "..."
                }
            ]
        """
        # 使用LangChain的相似度搜索
        docs_with_scores = self.vectorstore.similarity_search_with_score(
            query,
            k=top_k,
            filter=filter_dict
        )
        
        results = []
        for doc, score in docs_with_scores:
            # 过滤掉初始化文档
            if doc.metadata.get("type") == "init":
                continue
                
            results.append({
                "skill_id": doc.metadata.get("skill_id"),
                "name": doc.metadata.get("name"),
                "score": float(1 - score),  # FAISS返回的是距离，转换为相似度
                "content": doc.page_content
            })
        
        return results
    
    def save(self):
        """保存索引到磁盘"""
        self.vectorstore.save_local(str(self.index_path))
    
    def get_stats(self) -> Dict:
        """获取向量库统计信息"""
        return {
            "total_documents": self.vectorstore.index.ntotal if hasattr(self.vectorstore, 'index') else 0,
            "index_path": str(self.index_path),
            "embedding_type": "API (NVIDIA/Cerebras)"
        }
