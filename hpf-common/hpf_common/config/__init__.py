"""
配置管理
"""
from pydantic import Field
from pydantic_settings import BaseSettings
from typing import Literal, Optional


class Settings(BaseSettings):
    """
    全局配置管理
    
    使用示例:
        from core.config import settings
        
        print(settings.llm.provider)  # nvidia
        print(settings.db.audit_db_path)  # housing_provident_fund.db
    """
    
    # === 环境配置 ===
    env: Literal["dev", "staging", "prod"] = "dev"
    
    # === LLM 配置 ===
    default_llm_provider: str = Field(default="nvidia", validation_alias="DEFAULT_LLM_PROVIDER")
    default_llm_model: str = Field(default="z-ai/glm4.7", validation_alias="DEFAULT_LLM_MODEL")
    nvidia_api_key: Optional[str] = Field(default=None, validation_alias="NVIDIA_API_KEY")
    openai_api_key: Optional[str] = Field(default=None, validation_alias="OPENAI_API_KEY")
    cerebras_api_key: Optional[str] = Field(default=None, validation_alias="CEREBRAS_API_KEY")
    
    # === 数据库配置 ===
    # 审计系统数据库 (SQLite)
    audit_db_path: str = Field(default="housing_provident_fund.db", validation_alias="DB_PATH")
    
    # 数据平台数据库 (DuckDB)
    duckdb_path: str = Field(default="data/warehouse.duckdb", validation_alias="DUCKDB_PATH")
    duckdb_dataset: str = Field(default="ods", validation_alias="DUCKDB_DATASET")
    
    # Oracle
    oracle_user: Optional[str] = Field(default=None, validation_alias="ORACLE_USER")
    oracle_password: Optional[str] = Field(default=None, validation_alias="ORACLE_PASSWORD")
    oracle_host: Optional[str] = Field(default="localhost", validation_alias="ORACLE_HOST")
    oracle_port: str = Field(default="1521", validation_alias="ORACLE_PORT")
    oracle_service: str = Field(default="ORCL", validation_alias="ORACLE_SERVICE")
    oracle_schema: Optional[str] = Field(default=None, validation_alias="ORACLE_SCHEMA")
    
    # === Embedding 配置 ===
    embedding_provider: str = Field(default="openai", validation_alias="EMBEDDING_PROVIDER")
    embedding_model: str = Field(default="text-embedding-3-small", validation_alias="EMBEDDING_MODEL")
    
    # === API配置 ===
    api_prefix: str = Field(default="", validation_alias="API_PREFIX")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "ignore"


# 全局单例
settings = Settings()


__all__ = ["settings", "Settings"]
