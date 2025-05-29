#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化的Supabase连接测试
"""

import asyncio
import os
import sys
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

async def test_supabase_connection():
    """测试Supabase连接"""
    print("🔗 测试Supabase连接...")
    
    try:
        from config.supabase_config import supabase_config
        
        if supabase_config.is_connected():
            print("✅ Supabase连接成功!")
            return True
        else:
            print("❌ Supabase连接失败!")
            return False
            
    except Exception as e:
        print(f"❌ 初始化Supabase客户端时出错: {e}")
        return False

async def test_db_store_basic():
    """测试XhsDbStoreImplement的基本功能"""
    print("\n🗃️ 测试DB存储类...")
    
    try:
        from store.xhs.xhs_store_impl import XhsDbStoreImplement
        
        store = XhsDbStoreImplement()
        
        # 测试内容存储
        test_content = {
            "note_id": "simple_test_note_001",
            "type": "normal",
            "title": "简单测试笔记",
            "desc": "这是一个简单的测试笔记",
            "video_url": "",
            "last_update_time": "2025-05-29 10:00:00",
            "author_id": "simple_test_user_001",
            "nickname": "简单测试用户",
            "liked_count": "10",
            "collected_count": "5",
            "comment_count": "3",
            "share_count": "2",
            "image_list": "",
            "tag_list": "测试",
            "note_url": "https://www.xiaohongshu.com/explore/simple_test_note_001",
        }
        
        await store.store_content(test_content)
        print("✅ 成功测试内容存储")
        
        # 测试搜索结果存储
        test_search = {
            "keyword": "简单测试",
            "search_account": "简单测试用户",
            "rank": 1,
            "note_id": "simple_test_note_001",
        }
        
        await store.store_search_result(test_search)
        print("✅ 成功测试搜索结果存储")
        
        return True
        
    except Exception as e:
        print(f"❌ 测试DB存储类时出错: {e}")
        return False

async def main():
    """主测试函数"""
    print("🚀 开始简化Supabase测试...\n")
    
    # 检查环境变量
    print("🔍 检查环境变量...")
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_ANON_KEY')
    
    if supabase_url and supabase_key:
        print("✅ 环境变量配置正确")
        print(f"📡 URL: {supabase_url[:50]}...")
        print(f"🔑 Key: {supabase_key[:20]}...")
    else:
        print("❌ 环境变量未配置")
        return
    
    # 测试连接
    connected = await test_supabase_connection()
    
    if connected:
        # 测试基本功能
        await test_db_store_basic()
        print("\n🎉 简化测试完成!")
    else:
        print("\n❌ 由于连接失败，跳过后续测试")

if __name__ == "__main__":
    asyncio.run(main()) 