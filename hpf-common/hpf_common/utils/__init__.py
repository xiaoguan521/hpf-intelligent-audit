"""
通用工具函数
"""
import logging
import sys
from datetime import datetime
from typing import Optional


def setup_logger(
    name: str = "hpf",
    level: int = logging.INFO,
    log_file: Optional[str] = None
) -> logging.Logger:
    """
    配置日志
    
    Args:
        name: logger名称
        level: 日志级别
        log_file: 日志文件路径 (可选)
    
    Returns:
        配置好的logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # 避免重复添加handler
    if logger.handlers:
        return logger
    
    # 格式化
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 控制台handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # 文件handler (可选)
    if log_file:
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def format_datetime(dt: Optional[datetime] = None, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """
    格式化日期时间
    
    Args:
        dt: datetime对象,默认为当前时间
        fmt: 格式字符串
    
    Returns:
        格式化后的字符串
    """
    if dt is None:
        dt = datetime.now()
    return dt.strftime(fmt)


def safe_divide(a: float, b: float, default: float = 0.0) -> float:
    """
    安全除法,避免除零错误
    
    Args:
        a: 被除数
        b: 除数
        default: 除零时的默认返回值
    
    Returns:
        计算结果或默认值
    """
    try:
        return a / b if b != 0 else default
    except (TypeError, ZeroDivisionError):
        return default


__all__ = ["setup_logger", "format_datetime", "safe_divide"]
