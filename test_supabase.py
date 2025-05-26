#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试Supabase连接和表操作
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
            client = supabase_config.client
            
            # 测试简单查询
            try:
                result = client.table("note_detail").select("count", count="exact").execute()
                print(f"📊 note_detail表记录数: {result.count if hasattr(result, 'count') else 'N/A'}")
            except Exception as e:
                print(f"⚠️ 查询note_detail表时出错: {e}")
            
            return True
        else:
            print("❌ Supabase连接失败!")
            return False
            
    except Exception as e:
        print(f"❌ 初始化Supabase客户端时出错: {e}")
        return False

async def test_note_detail_operations():
    """测试笔记详情表操作"""
    print("\n📝 测试note_detail表操作...")
    
    try:
        from store.xhs.xhs_store_sql import supa_insert_note_detail, supa_query_note_by_id
        
        # 测试数据
        test_note = {
            "note_id": "test_note_123",
            "type": "normal",
            "title": "测试笔记标题",
            "desc": "这是一个测试笔记的描述内容",
            "video_url": "",
            "time": int(datetime.now().timestamp() * 1000),
            "last_update_time": int(datetime.now().timestamp() * 1000),
            "user_id": "test_user_123",
            "nickname": "测试用户",
            "avatar": "https://example.com/avatar.jpg",
            "liked_count": "100+",
            "collected_count": "50+",
            "comment_count": "20+",
            "share_count": "10+",
            "ip_location": "上海",
            "image_list": "https://example.com/image1.jpg,https://example.com/image2.jpg",
            "tag_list": "测试,Supabase",
            "note_url": "https://www.xiaohongshu.com/explore/test_note_123",
            "source_keyword": "测试关键词",
            "xsec_token": "test_token_123",
        }
        
        # 插入测试数据
        success = await supa_insert_note_detail(test_note)
        if success:
            print("✅ 成功插入测试笔记数据")
            
            # 查询测试数据
            result = await supa_query_note_by_id("test_note_123")
            if result:
                print(f"✅ 成功查询到笔记: {result.get('title')}")
            else:
                print("❌ 查询笔记失败")
        else:
            print("❌ 插入笔记数据失败")
            
    except Exception as e:
        print(f"❌ 测试note_detail操作时出错: {e}")

async def test_author_detail_operations():
    """测试作者详情表操作"""
    print("\n👤 测试author_detail表操作...")
    
    try:
        from store.xhs.xhs_store_sql import supa_insert_author_detail, supa_query_author_by_id
        
        # 测试数据
        test_author = {
            "user_id": "test_user_123",
            "nickname": "测试用户",
            "avatar": "https://example.com/avatar.jpg",
            "desc": "这是一个测试用户的简介",
            "gender": "female",
            "follows": 100,
            "fans": 1000,
            "interaction": 50000,
            "ip_location": "上海",
        }
        
        # 插入测试数据
        success = await supa_insert_author_detail(test_author)
        if success:
            print("✅ 成功插入测试作者数据")
            
            # 查询测试数据
            result = await supa_query_author_by_id("test_user_123")
            if result:
                print(f"✅ 成功查询到作者: {result.get('nickname')}")
            else:
                print("❌ 查询作者失败")
        else:
            print("❌ 插入作者数据失败")
            
    except Exception as e:
        print(f"❌ 测试author_detail操作时出错: {e}")

async def test_comment_detail_operations():
    """测试评论详情表操作"""
    print("\n💬 测试comment_detail表操作...")
    
    try:
        from store.xhs.xhs_store_sql import supa_insert_comment_detail, supa_query_comment_by_id
        
        # 测试数据
        test_comment = {
            "comment_id": "test_comment_123",
            "note_id": "test_note_123",
            "content": "这是一个测试评论",
            "user_id": "test_user_123",
            "nickname": "测试用户",
            "avatar": "https://example.com/avatar.jpg",
            "create_time": int(datetime.now().timestamp() * 1000),
            "like_count": 10,
            "pictures": "https://example.com/comment_pic.jpg",
            "parent_comment_id": 0,
            "is_author": False,
            "ip_location": "上海",
        }
        
        # 插入测试数据
        success = await supa_insert_comment_detail(test_comment)
        if success:
            print("✅ 成功插入测试评论数据")
            
            # 查询测试数据
            result = await supa_query_comment_by_id("test_comment_123")
            if result:
                print(f"✅ 成功查询到评论: {result.get('content')}")
            else:
                print("❌ 查询评论失败")
        else:
            print("❌ 插入评论数据失败")
            
    except Exception as e:
        print(f"❌ 测试comment_detail操作时出错: {e}")

async def test_search_result_operations():
    """测试搜索结果表操作"""
    print("\n🔍 测试search_result表操作...")
    
    try:
        from store.xhs.xhs_store_sql import supa_insert_search_result
        
        # 测试数据
        test_search = {
            "keyword": "测试关键词",
            "rank": 1,
            "note_id": "test_note_123",
            "create_time": datetime.now().isoformat(),
        }
        
        # 插入测试数据
        success = await supa_insert_search_result(test_search)
        if success:
            print("✅ 成功插入测试搜索结果数据")
        else:
            print("❌ 插入搜索结果数据失败")
            
    except Exception as e:
        print(f"❌ 测试search_result操作时出错: {e}")

async def test_db_store_with_supabase():
    """测试XhsDbStoreImplement的Supabase集成"""
    print("\n🗃️ 测试DB存储类的Supabase集成...")
    
    try:
        from store.xhs.xhs_store_impl import XhsDbStoreImplement
        
        store = XhsDbStoreImplement()
        
        # 测试内容存储
        test_content = {
            "note_id": "db_test_note_789",
            "type": "normal",
            "title": "DB测试笔记",
            "desc": "通过DB存储类测试的笔记",
            "video_url": "",
            "time": int(datetime.now().timestamp() * 1000),
            "last_update_time": int(datetime.now().timestamp() * 1000),
            "user_id": "db_test_user_789",
            "nickname": "DB测试用户",
            "avatar": "https://example.com/db_avatar.jpg",
            "liked_count": "300+",
            "collected_count": "150+",
            "comment_count": "75+",
            "share_count": "30+",
            "ip_location": "深圳",
            "image_list": "",
            "tag_list": "DB测试",
            "note_url": "https://www.xiaohongshu.com/explore/db_test_note_789",
            "source_keyword": "DB测试",
            "xsec_token": "db_test_token",
        }
        
        await store.store_content(test_content)
        print("✅ 通过DB存储类成功保存内容到MySQL和Supabase")
        
        # 测试作者存储
        test_author = {
            "user_id": "db_test_user_789",
            "nickname": "DB测试用户",
            "avatar": "https://example.com/db_avatar.jpg",
            "desc": "这是一个DB测试用户的简介",
            "gender": "male",
            "follows": 200,
            "fans": 2000,
            "interaction": 100000,
            "ip_location": "深圳",
        }
        
        await store.store_creator(test_author)
        print("✅ 通过DB存储类成功保存作者到MySQL和Supabase")
        
        # 测试评论存储
        test_comment = {
            "comment_id": "db_test_comment_789",
            "note_id": "db_test_note_789",
            "content": "这是一个DB测试评论",
            "user_id": "db_test_user_789",
            "nickname": "DB测试用户",
            "avatar": "https://example.com/db_avatar.jpg",
            "create_time": int(datetime.now().timestamp() * 1000),
            "like_count": 15,
            "pictures": "",
            "parent_comment_id": 0,
            "is_author": False,
            "ip_location": "深圳",
        }
        
        await store.store_comment(test_comment)
        print("✅ 通过DB存储类成功保存评论到MySQL和Supabase")
        
        # 测试搜索结果存储
        test_search = {
            "keyword": "DB测试关键词",
            "rank": 1,
            "note_id": "db_test_note_789",
            "create_time": datetime.now().isoformat(),
        }
        
        await store.store_search_result(test_search)
        print("✅ 通过DB存储类成功保存搜索结果到Supabase")
        
    except Exception as e:
        print(f"❌ 测试DB存储类集成时出错: {e}")

async def test_json_store_with_supabase():
    """测试XhsJsonStoreImplement的纯JSON存储（不包含Supabase）"""
    print("\n🗂️ 测试JSON存储类（纯JSON存储）...")
    
    try:
        from store.xhs.xhs_store_impl import XhsJsonStoreImplement
        
        store = XhsJsonStoreImplement()
        
        # 测试内容存储
        test_content = {
            "note_id": "json_test_note_456",
            "type": "normal",
            "title": "JSON测试笔记",
            "desc": "通过JSON存储类测试的笔记",
            "video_url": "",
            "time": int(datetime.now().timestamp() * 1000),
            "last_update_time": int(datetime.now().timestamp() * 1000),
            "user_id": "json_test_user_456",
            "nickname": "JSON测试用户",
            "avatar": "https://example.com/json_avatar.jpg",
            "liked_count": "200+",
            "collected_count": "100+",
            "comment_count": "50+",
            "share_count": "20+",
            "ip_location": "北京",
            "image_list": "",
            "tag_list": "JSON测试",
            "note_url": "https://www.xiaohongshu.com/explore/json_test_note_456",
            "source_keyword": "JSON测试",
            "xsec_token": "json_test_token",
        }
        
        await store.store_content(test_content)
        print("✅ 通过JSON存储类成功保存内容到JSON文件")
        
    except Exception as e:
        print(f"❌ 测试JSON存储类时出错: {e}")

async def main():
    """主测试函数"""
    print("🚀 开始Supabase集成测试...\n")
    
    # 测试连接
    connected = await test_supabase_connection()
    
    if connected:
        # 测试各表操作
        await test_note_detail_operations()
        await test_author_detail_operations()
        await test_comment_detail_operations()
        await test_search_result_operations()
        
        # 测试DB存储类集成（MySQL + Supabase）
        await test_db_store_with_supabase()
        
        # 测试JSON存储类（纯JSON）
        await test_json_store_with_supabase()
        
        print("\n🎉 所有测试完成!")
    else:
        print("\n❌ 由于连接失败，跳过后续测试")

if __name__ == "__main__":
    asyncio.run(main()) 