# -*- coding: utf-8 -*-
"""
HttpX 兼容性工具
处理不同版本HttpX中代理参数的差异
"""

import httpx
from typing import Optional, Dict, Any
from packaging import version

def get_httpx_version():
    """获取当前httpx版本"""
    return version.parse(httpx.__version__)

def create_httpx_client(proxies: Optional[Dict] = None, **kwargs) -> httpx.AsyncClient:
    """
    创建httpx.AsyncClient，自动处理不同版本的代理参数差异
    
    Args:
        proxies: 代理配置字典，格式为 {"http://": "proxy_url", "https://": "proxy_url"}
        **kwargs: 其他httpx.AsyncClient参数
        
    Returns:
        httpx.AsyncClient实例
    """
    current_version = get_httpx_version()
    
    # httpx 0.26.0 之前使用 proxies 参数
    if current_version < version.parse("0.26.0"):
        if proxies:
            kwargs['proxies'] = proxies
        return httpx.AsyncClient(**kwargs)
    
    # httpx 0.26.0+ 使用 proxy 参数（单数形式）
    else:
        if proxies:
            # 如果提供了proxies字典，转换为新的proxy格式
            # 优先使用https代理，如果没有则使用http代理
            proxy_url = proxies.get("https://") or proxies.get("http://")
            if proxy_url:
                kwargs['proxy'] = proxy_url
        return httpx.AsyncClient(**kwargs)

def create_sync_httpx_client(proxies: Optional[Dict] = None, **kwargs) -> httpx.Client:
    """
    创建httpx.Client，自动处理不同版本的代理参数差异
    
    Args:
        proxies: 代理配置字典
        **kwargs: 其他httpx.Client参数
        
    Returns:
        httpx.Client实例
    """
    current_version = get_httpx_version()
    
    if current_version < version.parse("0.26.0"):
        if proxies:
            kwargs['proxies'] = proxies
        return httpx.Client(**kwargs)
    else:
        if proxies:
            proxy_url = proxies.get("https://") or proxies.get("http://")
            if proxy_url:
                kwargs['proxy'] = proxy_url
        return httpx.Client(**kwargs)

def create_httpx_async_context(proxies: Optional[Dict] = None, **kwargs):
    """
    创建httpx异步上下文管理器
    
    Args:
        proxies: 代理配置字典
        **kwargs: 其他httpx.AsyncClient参数
        
    Returns:
        httpx.AsyncClient上下文管理器
    """
    return create_httpx_client(proxies=proxies, **kwargs)

def format_proxy_for_httpx(proxy_dict: Optional[Dict]) -> Optional[str]:
    """
    将代理字典格式化为httpx 0.26.0+需要的格式
    
    Args:
        proxy_dict: 代理字典，例如 {"http://": "http://user:pass@host:port"}
        
    Returns:
        代理URL字符串
    """
    if not proxy_dict:
        return None
    
    # 优先使用https代理，如果没有则使用http代理
    return proxy_dict.get("https://") or proxy_dict.get("http://") 