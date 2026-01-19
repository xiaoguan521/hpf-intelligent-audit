"""
Embedding 客户端
"""
from typing import List, Union
import os


class EmbeddingClient:
    """
    统一的 Embedding 客户端
    
    支持: OpenAI, Cerebras, NVIDIA
    
    使用示例:
        from core.embedding import EmbeddingClient
        
        client = EmbeddingClient(provider="openai")
        vectors = client.embed(["文本1", "文本2"])
    """
    
    def __init__(
        self,
        provider: str = None,
        model: str = None,
        api_key: str = None,
        **kwargs
    ):
        self.provider = provider or os.getenv("EMBEDDING_PROVIDER", "openai")
        self.model = model or self._get_default_model()
        self.api_key = api_key or self._get_api_key()
        self.kwargs = kwargs
        
        self._client = self._load_client()
    
    def _get_default_model(self) -> str:
        """获取默认模型"""
        model_map = {
            "openai": os.getenv("EMBEDDING_MODEL", "text-embedding-3-small"),
            "cerebras": os.getenv("EMBEDDING_MODEL", "text-embedding-3-small"),
            "nvidia": os.getenv("NVIDIA_EMBEDDING_MODEL", "NV-Embed-QA"),
        }
        return model_map.get(self.provider, "text-embedding-3-small")
    
    def _get_api_key(self) -> str:
        """获取 API 密钥"""
        key_map = {
            "openai": "OPENAI_API_KEY",
            "cerebras": "CEREBRAS_API_KEY",
            "nvidia": "NVIDIA_API_KEY",
        }
        env_key = key_map.get(self.provider)
        return os.getenv(env_key, "")
    
    def _load_client(self):
        """加载客户端"""
        if self.provider in ["openai", "cerebras"]:
            try:
                from openai import OpenAI
                base_url_map = {
                    "openai": os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1"),
                    "cerebras": os.getenv("CEREBRAS_BASE_URL", "https://api.cerebras.ai/v1"),
                }
                return OpenAI(
                    api_key=self.api_key,
                    base_url=base_url_map.get(self.provider)
                )
            except ImportError:
                raise ImportError("请安装 openai: pip install openai")
        elif self.provider == "nvidia":
            try:
                from openai import OpenAI
                return OpenAI(
                    api_key=self.api_key,
                    base_url="https://integrate.api.nvidia.com/v1"
                )
            except ImportError:
                raise ImportError("请安装 openai: pip install openai")
        else:
            raise ValueError(f"Unsupported provider: {self.provider}")
    
    def embed(self, texts: Union[str, List[str]]) -> List[List[float]]:
        """
        生成向量
        
        Args:
            texts: 单个文本或文本列表
        
        Returns:
            向量列表
        """
        if isinstance(texts, str):
            texts = [texts]
        
        response = self._client.embeddings.create(
            model=self.model,
            input=texts
        )
        
        return [item.embedding for item in response.data]


__all__ = ["EmbeddingClient"]
