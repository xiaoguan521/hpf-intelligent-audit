"""
LLM 客户端统一接口
"""
from typing import List, Dict, Optional, Generator
from abc import ABC, abstractmethod
import os


class BaseLLMProvider(ABC):
    """LLM Provider 基类"""
    
    @abstractmethod
    def chat(self, messages: List[Dict[str, str]], **kwargs) -> str:
        """发送聊天请求"""
        pass
    
    @abstractmethod
    def stream_chat(self, messages: List[Dict[str, str]], **kwargs) -> Generator[str, None, None]:
        """流式聊天"""
        pass


class LLMClient:
    """
    统一的 LLM 客户端接口
    
    支持多个 Provider: nvidia, openai, cerebras, anthropic
    
    使用示例:
        from core.llm import LLMClient
        
        client = LLMClient(provider="nvidia")
        response = client.chat([{"role": "user", "content": "你好"}])
    """
    
    def __init__(
        self,
        provider: Optional[str] = None,
        model: Optional[str] = None,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        **kwargs
    ):
        """
        初始化 LLM 客户端
        
        Args:
            provider: LLM提供商 (nvidia, openai, cerebras, anthropic)
            model: 模型名称
            api_key: API密钥
            base_url: API基础URL
            **kwargs: 其他参数
        """
        self.provider = provider or os.getenv("DEFAULT_LLM_PROVIDER", "nvidia")
        self.model = model or self._get_default_model()
        self.api_key = api_key or self._get_api_key()
        self.base_url = base_url or self._get_base_url()
        self.kwargs = kwargs
        
        # 加载具体的 provider 实现
        self._client = self._load_provider()
    
    def _get_default_model(self) -> str:
        """获取默认模型"""
        model_map = {
            "nvidia": os.getenv("DEFAULT_LLM_MODEL", "z-ai/glm4.7"),
            "openai": os.getenv("OPENAI_MODEL", os.getenv("DEFAULT_LLM_MODEL", "gpt-4o-mini")),
            "cerebras": os.getenv("CEREBRAS_MODEL", "zai-glm-4.7"),
            "anthropic": os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-20241022"),
        }
        return model_map.get(self.provider, "z-ai/glm4.7")
    
    def _get_api_key(self) -> str:
        """获取 API 密钥"""
        key_map = {
            "nvidia": "NVIDIA_API_KEY",
            "openai": "OPENAI_API_KEY",
            "cerebras": "CEREBRAS_API_KEY",
            "anthropic": "ANTHROPIC_API_KEY",
        }
        env_key = key_map.get(self.provider)
        return os.getenv(env_key, "")
    
    def _get_base_url(self) -> str:
        """获取 Base URL"""
        url_map = {
            "nvidia": os.getenv("NVIDIA_BASE_URL", "https://integrate.api.nvidia.com/v1"),
            "openai": os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1"),
            "cerebras": os.getenv("CEREBRAS_BASE_URL", "https://api.cerebras.ai/v1"),
            "anthropic": os.getenv("ANTHROPIC_BASE_URL", "https://api.anthropic.com"),
        }
        return url_map.get(self.provider, "")
    
    def _load_provider(self) -> BaseLLMProvider:
        """动态加载 Provider 实现"""
        if self.provider in ["nvidia", "openai", "cerebras"]:
            # 这些都使用 OpenAI 兼容接口
            from hpf_common.llm.providers import OpenAICompatibleProvider
            return OpenAICompatibleProvider(
                api_key=self.api_key,
                base_url=self.base_url,
                model=self.model,
                **self.kwargs
            )
        elif self.provider == "anthropic":
            from hpf_common.llm.providers import AnthropicProvider
            return AnthropicProvider(
                api_key=self.api_key,
                model=self.model,
                **self.kwargs
            )
        else:
            raise ValueError(f"Unsupported provider: {self.provider}")
    
    def chat(self, messages: List[Dict[str, str]], **kwargs) -> str:
        """
        发送聊天请求
        
        Args:
            messages: 消息列表 [{"role": "user", "content": "..."}]
            **kwargs: 其他参数 (temperature, max_tokens等)
        
        Returns:
            str: LLM 响应内容
        """
        return self._client.chat(messages, **kwargs)
    
    def stream_chat(self, messages: List[Dict[str, str]], **kwargs) -> Generator[str, None, None]:
        """
        流式聊天
        
        Args:
            messages: 消息列表
            **kwargs: 其他参数
        
        Yields:
            str: 流式响应片段
        """
        return self._client.stream_chat(messages, **kwargs)


__all__ = ["LLMClient", "BaseLLMProvider"]
