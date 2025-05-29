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
# @Time    : 2024/4/6 15:30
# @Desc    : sql接口集合

from typing import Dict, List, Optional

from db import AsyncMysqlDB
from var import media_crawler_db_var


async def query_content_by_content_id(content_id: str) -> Dict:
    """
    查询一条内容记录（xhs的帖子 ｜ 抖音的视频 ｜ 微博 ｜ 快手视频 ...）
    Args:
        content_id:

    Returns:

    """
    async_db_conn: AsyncMysqlDB = media_crawler_db_var.get()
    sql: str = f"select * from xhs_note where note_id = '{content_id}'"
    rows: List[Dict] = await async_db_conn.query(sql)
    if len(rows) > 0:
        return rows[0]
    return dict()


async def add_new_content(content_item: Dict) -> int:
    """
    新增一条内容记录（xhs的帖子 ｜ 抖音的视频 ｜ 微博 ｜ 快手视频 ...）
    Args:
        content_item:

    Returns:

    """
    async_db_conn: AsyncMysqlDB = media_crawler_db_var.get()
    last_row_id: int = await async_db_conn.item_to_table("xhs_note", content_item)
    return last_row_id


async def update_content_by_content_id(content_id: str, content_item: Dict) -> int:
    """
    更新一条记录（xhs的帖子 ｜ 抖音的视频 ｜ 微博 ｜ 快手视频 ...）
    Args:
        content_id:
        content_item:

    Returns:

    """
    async_db_conn: AsyncMysqlDB = media_crawler_db_var.get()
    effect_row: int = await async_db_conn.update_table("xhs_note", content_item, "note_id", content_id)
    return effect_row



async def query_comment_by_comment_id(comment_id: str) -> Dict:
    """
    查询一条评论内容
    Args:
        comment_id:

    Returns:

    """
    async_db_conn: AsyncMysqlDB = media_crawler_db_var.get()
    sql: str = f"select * from xhs_note_comment where comment_id = '{comment_id}'"
    rows: List[Dict] = await async_db_conn.query(sql)
    if len(rows) > 0:
        return rows[0]
    return dict()


async def add_new_comment(comment_item: Dict) -> int:
    """
    新增一条评论记录
    Args:
        comment_item:

    Returns:

    """
    async_db_conn: AsyncMysqlDB = media_crawler_db_var.get()
    last_row_id: int = await async_db_conn.item_to_table("xhs_note_comment", comment_item)
    return last_row_id


async def update_comment_by_comment_id(comment_id: str, comment_item: Dict) -> int:
    """
    更新增一条评论记录
    Args:
        comment_id:
        comment_item:

    Returns:

    """
    async_db_conn: AsyncMysqlDB = media_crawler_db_var.get()
    effect_row: int = await async_db_conn.update_table("xhs_note_comment", comment_item, "comment_id", comment_id)
    return effect_row


async def query_creator_by_user_id(user_id: str) -> Dict:
    """
    查询一条创作者记录
    Args:
        user_id:

    Returns:

    """
    async_db_conn: AsyncMysqlDB = media_crawler_db_var.get()
    sql: str = f"select * from xhs_creator where user_id = '{user_id}'"
    rows: List[Dict] = await async_db_conn.query(sql)
    if len(rows) > 0:
        return rows[0]
    return dict()


async def add_new_creator(creator_item: Dict) -> int:
    """
    新增一条创作者信息
    Args:
        creator_item:

    Returns:

    """
    async_db_conn: AsyncMysqlDB = media_crawler_db_var.get()
    last_row_id: int = await async_db_conn.item_to_table("xhs_creator", creator_item)
    return last_row_id


async def update_creator_by_user_id(user_id: str, creator_item: Dict) -> int:
    """
    更新一条创作者信息
    Args:
        user_id:
        creator_item:

    Returns:

    """
    async_db_conn: AsyncMysqlDB = media_crawler_db_var.get()
    effect_row: int = await async_db_conn.update_table("xhs_creator", creator_item, "user_id", user_id)
    return effect_row

# Supabase相关操作函数
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from config.supabase_config import supabase_config
    from tools import utils
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False
    utils = None

async def supa_insert_note_detail(note_item: Dict) -> bool:
    """
    插入或更新笔记详情到Supabase
    Args:
        note_item: 笔记信息字典

    Returns:
        bool: 操作是否成功
    """
    if not SUPABASE_AVAILABLE or not supabase_config.is_connected():
        if utils:
            utils.logger.warning("Supabase not available, skipping note_detail insert")
        return False
    
    try:
        client = supabase_config.client
        
        # 准备数据 - 基于demo文件结构
        data = {
            "note_id": note_item.get("note_id"),
            "title": note_item.get("title"),
            "type": note_item.get("type"),
            "desc": note_item.get("desc"),
            "note_url": note_item.get("note_url", ""),
            "video_url": note_item.get("video_url", ""),
            "last_update_time": note_item.get("last_update_time"),
            "author_id": note_item.get("author_id"),
            "nickname": note_item.get("nickname"),
            "liked_count": note_item.get("liked_count", ""),
            "collected_count": note_item.get("collected_count", ""),
            "comment_count": note_item.get("comment_count", ""),
            "share_count": note_item.get("share_count", ""),
            "image_list": note_item.get("image_list", ""),
            "tag_list": note_item.get("tag_list", ""),
        }
        
        # 使用upsert避免重复插入
        result = client.table("xhs_note").upsert(data, 
            on_conflict="note_id"
        ).execute()
        
        if utils:
            utils.logger.info(f"Successfully upserted xhs_note for note_id: {note_item.get('note_id')}")
        return True
        
    except Exception as e:
        if utils:
            utils.logger.error(f"Failed to upsert xhs_note: {e}")
        return False

async def supa_insert_author_detail(author_item: Dict) -> bool:
    """
    插入或更新作者详情到Supabase
    Args:
        author_item: 作者信息字典

    Returns:
        bool: 操作是否成功
    """
    if not SUPABASE_AVAILABLE or not supabase_config.is_connected():
        if utils:
            utils.logger.warning("Supabase not available, skipping author_detail insert")
        return False
    
    try:
        client = supabase_config.client
        
        # 准备数据
        data = {
            "user_id": author_item.get("user_id"),
            "nickname": author_item.get("nickname"),
            "avatar": author_item.get("avatar", ""),
            "desc": author_item.get("desc", ""),
            "gender": author_item.get("gender", ""),
            "follows": author_item.get("follows", 0),
            "fans": author_item.get("fans", 0),
            "interaction": author_item.get("interaction", 0),
            "ip_location": author_item.get("ip_location", ""),
        }
        
        # 使用upsert避免重复插入
        result = client.table("xhs_author").upsert(data, 
            on_conflict="user_id"
        ).execute()
        
        if utils:
            utils.logger.info(f"Successfully upserted author_detail for user_id: {author_item.get('user_id')}")
        return True
        
    except Exception as e:
        if utils:
            utils.logger.error(f"Failed to upsert author_detail: {e}")
        return False

async def supa_insert_comment_detail(comment_item: Dict) -> bool:
    """
    插入或更新评论详情到Supabase
    Args:
        comment_item: 评论信息字典

    Returns:
        bool: 操作是否成功
    """
    if not SUPABASE_AVAILABLE or not supabase_config.is_connected():
        if utils:
            utils.logger.warning("Supabase not available, skipping comment_detail insert")
        return False
    
    try:
        client = supabase_config.client
        
        # 准备数据
        data = {
            "comment_id": comment_item.get("comment_id"),
            "note_id": comment_item.get("note_id"),
            "content": comment_item.get("content", ""),
            "user_id": comment_item.get("user_id"),
            "nickname": comment_item.get("nickname"),
            "avatar": comment_item.get("avatar", ""),
            "create_time": comment_item.get("create_time"),
            "like_count": comment_item.get("like_count", 0),
            "pictures": comment_item.get("pictures", ""),
            "parent_comment_id": comment_item.get("parent_comment_id"),
            "is_author": comment_item.get("is_author", False),
            "ip_location": comment_item.get("ip_location", ""),
        }
        
        # 使用upsert避免重复插入
        result = client.table("xhs_comment_detail").upsert(data, 
            on_conflict="comment_id"
        ).execute()
        
        if utils:
            utils.logger.info(f"Successfully upserted comment_detail for comment_id: {comment_item.get('comment_id')}")
        return True
        
    except Exception as e:
        if utils:
            utils.logger.error(f"Failed to upsert comment_detail: {e}")
        return False

async def supa_insert_search_result(search_item: Dict) -> bool:
    """
    插入搜索结果到Supabase
    Args:
        search_item: 搜索结果字典，应包含keyword, rank, note_id等字段

    Returns:
        bool: 操作是否成功
    """
    if not SUPABASE_AVAILABLE or not supabase_config.is_connected():
        if utils:
            utils.logger.warning("Supabase not available, skipping search_result insert")
        return False
    
    try:
        client = supabase_config.client
        
        # 准备数据
        data = {
            "keyword": search_item.get("keyword"),
            "search_account": search_item.get("search_account"),
            "rank": search_item.get("rank"),
            "note_id": search_item.get("note_id"),
        }
        
        # 插入搜索结果
        result = client.table("xhs_search_result").insert(data).execute()
        
        if utils:
            utils.logger.info(f"Successfully inserted search_result for keyword: {search_item.get('keyword')}, note_id: {search_item.get('note_id')}")
        return True
        
    except Exception as e:
        if utils:
            utils.logger.error(f"Failed to insert search_result: {e}")
        return False

async def supa_query_note_by_id(note_id: str) -> Optional[Dict]:
    """
    根据note_id查询笔记详情
    Args:
        note_id: 笔记ID

    Returns:
        Dict: 笔记详情，如果不存在返回None
    """
    if not SUPABASE_AVAILABLE or not supabase_config.is_connected():
        return None
    
    try:
        client = supabase_config.client
        result = client.table("xhs_note").select("*").eq("note_id", note_id).execute()
        
        if result.data:
            return result.data[0]
        return None
        
    except Exception as e:
        if utils:
            utils.logger.error(f"Failed to query note_detail: {e}")
        return None

async def supa_query_author_by_id(user_id: str) -> Optional[Dict]:
    """
    根据user_id查询作者详情
    Args:
        user_id: 用户ID

    Returns:
        Dict: 作者详情，如果不存在返回None
    """
    if not SUPABASE_AVAILABLE or not supabase_config.is_connected():
        return None
    
    try:
        client = supabase_config.client
        result = client.table("xhs_author").select("*").eq("user_id", user_id).execute()
        
        if result.data:
            return result.data[0]
        return None
        
    except Exception as e:
        if utils:
            utils.logger.error(f"Failed to query author_detail: {e}")
        return None

async def supa_query_comment_by_id(comment_id: str) -> Optional[Dict]:
    """
    根据comment_id查询评论详情
    Args:
        comment_id: 评论ID

    Returns:
        Dict: 评论详情，如果不存在返回None
    """
    if not SUPABASE_AVAILABLE or not supabase_config.is_connected():
        return None
    
    try:
        client = supabase_config.client
        result = client.table("xhs_comment_detail").select("*").eq("comment_id", comment_id).execute()
        
        if result.data:
            return result.data[0]
        return None
        
    except Exception as e:
        if utils:
            utils.logger.error(f"Failed to query comment_detail: {e}")
        return None