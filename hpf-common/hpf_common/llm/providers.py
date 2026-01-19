"""
LLM Provider 具体实现
"""
from typing import List, Dict, Generator
from hpf_common.llm import BaseLLMProvider


class OpenAICompatibleProvider(BaseLLMProvider):
    """OpenAI 兼容接口的 Provider (支持 NVIDIA, OpenAI, Cerebras)"""
    
    def __init__(self, api_key: str, base_url: str, model: str, **kwargs):
        self.api_key = api_key
        self.base_url = base_url
        self.model = model
        self.kwargs = kwargs
        
        # 延迟导入，避免强制依赖
        try:
            from openai import OpenAI
            self.client = OpenAI(
                api_key=self.api_key,
                base_url=self.base_url
            )
        except ImportError:
            raise ImportError("请安装 openai 包: pip install openai")
    
    def chat(self, messages: List[Dict[str, str]], **kwargs) -> str:
        """发送聊天请求"""
        # 合并参数
        params = {**self.kwargs, **kwargs}
        params.setdefault("temperature", 0.7)
        params.setdefault("max_tokens", 2000)
        
        # 移除不支持的参数
        params.pop("verbose", None)
        params.pop("timeout", None)  # 某些客户端可能不支持 timeout 参数在此处传
        
        response = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            **params
        )
        
        return response.choices[0].message.content
    
    def stream_chat(self, messages: List[Dict[str, str]], **kwargs) -> Generator[str, None, None]:
        """流式聊天"""
        params = {**self.kwargs, **kwargs}
        params.setdefault("temperature", 0.7)
        params.setdefault("max_tokens", 2000)
        params["stream"] = True

        # 移除不支持的参数
        params.pop("verbose", None)
        params.pop("timeout", None)
        
        stream = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            **params
        )
        
        for chunk in stream:
            if chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content


class AnthropicProvider(BaseLLMProvider):
    """Anthropic Claude Provider"""
    
    def __init__(self, api_key: str, model: str, **kwargs):
        self.api_key = api_key
        self.model = model
        self.kwargs = kwargs
        
        try:
            from anthropic import Anthropic
            self.client = Anthropic(api_key=self.api_key)
        except ImportError:
            raise ImportError("请安装 anthropic 包: pip install anthropic")
    
    def chat(self, messages: List[Dict[str, str]], **kwargs) -> str:
        """发送聊天请求"""
        params = {**self.kwargs, **kwargs}
        params.setdefault("max_tokens", 2000)
        
        response = self.client.messages.create(
            model=self.model,
            messages=messages,
            **params
        )
        
        return response.content[0].text
    
    def stream_chat(self, messages: List[Dict[str, str]], **kwargs) -> Generator[str, None, None]:
        """流式聊天"""
        params = {**self.kwargs, **kwargs}
        params.setdefault("max_tokens", 2000)
        params["stream"] = True
        
        with self.client.messages.stream(
            model=self.model,
            messages=messages,
            **params
        ) as stream:
            for text in stream.text_stream:
                yield text


__all__ = ["OpenAICompatibleProvider", "AnthropicProvider"]
