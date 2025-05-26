# 声明：本代码仅供学习和研究目的使用。使用者应遵守以下原则：  
# 1. 不得用于任何商业用途。  
# 2. 使用时应遵守目标平台的使用条款和robots.txt规则。  
# 3. 不得进行大规模爬取或对平台造成运营干扰。  
# 4. 应合理控制请求频率，避免给目标平台带来不必要的负担。   
# 5. 不得用于任何非法或不当的用途。
#   
# 详细许可条款请参阅项目根目录下的LICENSE文件。  
# 使用本代码即表示您同意遵守上述原则和LICENSE中的所有条款。  

# -*- coding: utf-8 -*-
# @Author  : relakkes@gmail.com
# @Time    : 2025/1/25 12:00
# @Desc    : Supabase配置文件

import os
from supabase import create_client, Client
from typing import Optional
from tools import utils

class SupabaseConfig:
    _instance: Optional['SupabaseConfig'] = None
    _client: Optional[Client] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._client is None:
            self._init_client()
    
    def _init_client(self):
        """初始化Supabase客户端"""
        try:
            # 从环境变量获取配置
            url = os.getenv('SEO_SUPABASE_URL')
            anon_key = os.getenv('SEO_SUPABASE_ANON_KEY')
            
            if not url or not anon_key:
                raise ValueError("SEO_SUPABASE_URL and SEO_SUPABASE_ANON_KEY must be set in environment variables")
            
            self._client = create_client(url, anon_key)
            utils.logger.info("Supabase client initialized successfully")
            
        except Exception as e:
            utils.logger.error(f"Failed to initialize Supabase client: {e}")
            self._client = None
    
    @property
    def client(self) -> Optional[Client]:
        """获取Supabase客户端"""
        return self._client
    
    def is_connected(self) -> bool:
        """检查是否连接成功"""
        return self._client is not None

# 创建全局实例
supabase_config = SupabaseConfig() 