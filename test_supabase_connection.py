#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Supabase连接测试脚本

使用前请设置环境变量：
export SEO_SUPABASE_URL="https://your-project.supabase.co"
export SEO_SUPABASE_ANON_KEY="your-anon-key"
"""

import os
import asyncio
import sys

def test_environment_variables():
    """测试环境变量是否设置"""
    print("🔍 检查环境变量...")
    
    supabase_url = os.getenv('SEO_SUPABASE_URL')
    supabase_key = os.getenv('SEO_SUPABASE_ANON_KEY')
    
    if not supabase_url:
        print("❌ SEO_SUPABASE_URL 环境变量未设置")
        return False
    
    if not supabase_key:
        print("❌ SEO_SUPABASE_ANON_KEY 环境变量未设置")
        return False
    
    print(f"✅ SEO_SUPABASE_URL: {supabase_url}")
    print(f"✅ SEO_SUPABASE_ANON_KEY: {supabase_key[:20]}...")
    return True

async def test_supabase_connection():
    """测试Supabase连接"""
    try:
        print("\n🔗 测试Supabase连接...")
        
        # 导入Supabase配置
        from config.supabase_config import supabase_config
        
        if not supabase_config.is_connected():
            print("❌ Supabase客户端初始化失败")
            return False
        
        client = supabase_config.client
        
        # 测试连接 - 尝试查询表结构
        print("📋 检查数据库表...")
        
        # 测试xhs_note表
        try:
            result = client.table('xhs_note').select('note_id').limit(1).execute()
            print("✅ xhs_note 表连接成功")
        except Exception as e:
            print(f"⚠️  xhs_note 表可能不存在或无法访问: {e}")
        
        # 测试xhs_comment_detail表
        try:
            result = client.table('xhs_comment_detail').select('comment_id').limit(1).execute()
            print("✅ xhs_comment_detail 表连接成功")
        except Exception as e:
            print(f"⚠️  xhs_comment_detail 表可能不存在或无法访问: {e}")
        
        # 测试xhs_author表
        try:
            result = client.table('xhs_author').select('user_id').limit(1).execute()
            print("✅ xhs_author 表连接成功")
        except Exception as e:
            print(f"⚠️  xhs_author 表可能不存在或无法访问: {e}")
        
        print("\n✅ Supabase连接测试完成！")
        return True
        
    except Exception as e:
        print(f"❌ Supabase连接失败: {e}")
        return False

async def test_db_initialization():
    """测试数据库初始化逻辑"""
    try:
        print("\n🏗️  测试数据库初始化...")
        
        # 导入并测试数据库初始化
        import db
        await db.init_supabase_db()
        
        print("✅ 数据库初始化成功")
        return True
        
    except Exception as e:
        print(f"❌ 数据库初始化失败: {e}")
        return False

def print_setup_instructions():
    """打印设置说明"""
    print("""
📚 设置说明：

1. 创建Supabase项目：
   - 访问 https://supabase.com/dashboard
   - 创建新项目或选择现有项目

2. 获取API密钥：
   - 进入项目设置 > API
   - 复制 URL 和 anon/public key

3. 设置环境变量：
   export SEO_SUPABASE_URL="https://your-project.supabase.co"
   export SEO_SUPABASE_ANON_KEY="your-anon-key"

4. 创建数据表（如果不存在）：
   在Supabase SQL编辑器中运行SQL创建表结构。

5. 重新运行此测试脚本验证连接。
""")

async def main():
    """主函数"""
    print("🚀 Supabase连接测试开始\n")
    
    # 测试环境变量
    if not test_environment_variables():
        print_setup_instructions()
        sys.exit(1)
    
    # 测试Supabase连接
    if not await test_supabase_connection():
        print_setup_instructions()
        sys.exit(1)
    
    # 测试数据库初始化
    if not await test_db_initialization():
        sys.exit(1)
    
    print("\n🎉 所有测试通过！可以正常使用Supabase数据库存储。")

if __name__ == "__main__":
    asyncio.run(main()) 