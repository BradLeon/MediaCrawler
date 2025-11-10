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

def normalize_count_field(value: str) -> Optional[int]:
    """
    标准化计数字段，支持多种格式的转换：
    - "10+", "100+" -> 移除'+'号
    - "1.6万", "3万" -> 转换为实际数值（16000, 30000）
    - "1.2亿" -> 转换为实际数值（120000000）
    - "5千" -> 转换为实际数值（5000）

    Args:
        value: 原始值字符串或整数
    Returns:
        转换后的整数，如果无法转换则返回None
    """
    if not value:
        return None

    # 如果已经是整数，直接返回
    if isinstance(value, int):
        return value

    # 转换为字符串处理
    value_str = str(value).strip()

    # 1. 处理中文数字单位（万、亿、千）
    chinese_units = {
        '亿': 100000000,  # 1亿
        '万': 10000,       # 1万
        '千': 1000,        # 1千
        'k': 1000,         # 1k (有些平台用k)
        'K': 1000,         # 1K
        'w': 10000,        # 1w (有些平台用w代表万)
        'W': 10000,        # 1W
    }

    for unit, multiplier in chinese_units.items():
        if unit in value_str:
            try:
                # 提取数字部分："1.6万" -> "1.6"
                num_part = value_str.replace(unit, '').strip()
                # 转换为浮点数后乘以倍数
                num = float(num_part) if num_part else 1.0
                result = int(num * multiplier)
                return result
            except ValueError:
                continue

    # 2. 移除 '+' 符号并尝试转换
    if '+' in value_str:
        try:
            # "10+" -> 10
            return int(value_str.replace('+', ''))
        except ValueError:
            pass

    # 3. 尝试直接转换为整数
    try:
        return int(value_str)
    except ValueError:
        pass

    # 4. 尝试转换为浮点数再取整（处理小数情况）
    try:
        return int(float(value_str))
    except ValueError:
        return None

def has_fuzzy_data(note_item: Dict) -> bool:
    """
    检查笔记数据是否包含模糊数据，包括：
    - "10+" 格式
    - "1.6万"、"3万" 等中文数字格式
    - "1.2亿"、"5千" 等中文单位格式

    Args:
        note_item: 笔记信息字典
    Returns:
        bool: 是否包含模糊数据
    """
    fuzzy_fields = ['liked_count', 'collected_count', 'comment_count', 'share_count']
    fuzzy_indicators = ['+', '万', '亿', '千', 'k', 'K', 'w', 'W']

    for field in fuzzy_fields:
        value = note_item.get(field)
        if value and isinstance(value, str):
            # 检查是否包含任何模糊数据标志
            if any(indicator in value for indicator in fuzzy_indicators):
                return True

    return False

async def supa_upsert_note_fuzzy(note_item: Dict) -> bool:
    """
    插入或更新包含模糊数据的笔记到xhs_note_fuzzy表
    Args:
        note_item: 笔记信息字典（包含模糊数据如 "10+"）
    Returns:
        bool: 操作是否成功
    """
    if not SUPABASE_AVAILABLE or not supabase_config.is_connected():
        if utils:
            utils.logger.warning("Supabase not available, skipping note_fuzzy insert")
        return False

    try:
        client = supabase_config.client

        # xhs_note_fuzzy表的所有计数字段都是varchar，直接保存原始值
        if utils:
            utils.logger.info(f"Inserting fuzzy data to xhs_note_fuzzy for note_id: {note_item.get('note_id')}")

        # 使用on_conflict参数指定在note_id冲突时进行更新
        result = client.table("xhs_note_fuzzy").upsert(
            note_item,
            on_conflict="note_id"
        ).execute()

        if utils:
            utils.logger.info(f"Successfully upserted xhs_note_fuzzy for note_id: {note_item.get('note_id')}")
        return True

    except Exception as e:
        if utils:
            utils.logger.error(f"Failed to upsert xhs_note_fuzzy: {e}")
        return False

async def supa_upsert_note_detail(note_item: Dict) -> bool:
    """
    插入或更新笔记详情到Supabase
    支持自动fallback：如果数据包含模糊值（如 "10+"），会尝试转换或保存到fuzzy表
    Args:
        note_item: 笔记信息字典

    Returns:
        bool: 操作是否成功
    """
    if not SUPABASE_AVAILABLE or not supabase_config.is_connected():
        if utils:
            utils.logger.warning("Supabase not available, skipping note_detail insert")
        return False

    # 检查是否包含模糊数据
    if has_fuzzy_data(note_item):
        if utils:
            utils.logger.warning(f"Detected fuzzy data in note_item for note_id: {note_item.get('note_id')}")

        # 尝试清洗数据
        cleaned_item = note_item.copy()
        count_fields = ['liked_count', 'collected_count', 'comment_count', 'share_count']

        conversion_failed = False
        for field in count_fields:
            if field in cleaned_item:
                normalized_value = normalize_count_field(cleaned_item[field])
                if normalized_value is not None:
                    cleaned_item[field] = normalized_value
                    if utils:
                        utils.logger.info(f"Normalized {field}: {note_item[field]} -> {normalized_value}")
                else:
                    conversion_failed = True
                    if utils:
                        utils.logger.warning(f"Failed to normalize {field}: {note_item[field]}")

        # 如果转换成功，尝试插入到xhs_note表
        if not conversion_failed:
            try:
                client = supabase_config.client
                if utils:
                    utils.logger.info(f"Attempting to insert normalized data to xhs_note: {cleaned_item.get('note_id')}")

                result = client.table("xhs_note").upsert(
                    cleaned_item,
                    on_conflict="note_id"
                ).execute()

                if utils:
                    utils.logger.info(f"Successfully upserted xhs_note with normalized data for note_id: {cleaned_item.get('note_id')}")
                return True
            except Exception as e:
                if utils:
                    utils.logger.warning(f"Failed to upsert normalized data to xhs_note: {e}, falling back to xhs_note_fuzzy")

        # 如果转换失败或插入失败，fallback到xhs_note_fuzzy表
        return await supa_upsert_note_fuzzy(note_item)

    # 正常数据，直接插入到xhs_note表
    try:
        client = supabase_config.client
        utils.logger.info(f"insert note_item: {note_item}")

        # 使用on_conflict参数指定在note_id冲突时进行更新
        result = client.table("xhs_note").upsert(
            note_item,
            on_conflict="note_id"
        ).execute()

        if utils:
            utils.logger.info(f"Successfully upserted xhs_note for note_id: {note_item.get('note_id')}")
        return True

    except Exception as e:
        if utils:
            utils.logger.error(f"Failed to upsert xhs_note: {e}")
        # 最后的兜底：即使是正常数据，如果插入失败也尝试fuzzy表
        if utils:
            utils.logger.info(f"Attempting fallback to xhs_note_fuzzy for note_id: {note_item.get('note_id')}")
        return await supa_upsert_note_fuzzy(note_item)

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
        
        # 使用on_conflict参数指定在user_id冲突时进行更新
        result = client.table("xhs_author").upsert(
            data, 
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
            "sub_comment_count": comment_item.get("sub_comment_count", 0),
            #"ip_location": comment_item.get("ip_location", ""),
        }
        
        # 使用upsert避免重复插入
        result = client.table("xhs_comment").upsert(data, 
            on_conflict="comment_id"
        ).execute()
        
        if utils:
            utils.logger.info(f"Successfully upserted xhs_comment for comment_id: {comment_item.get('comment_id')}")
        return True
        
    except Exception as e:
        if utils:
            utils.logger.error(f"Failed to upsert comment_detail: {e}")
        return False

async def supa_insert_search_result(search_result_list: List[Dict]) -> bool:
    """
    批量插入搜索结果到Supabase
    Args:
        search_result_list: 搜索结果列表，每个元素应包含keyword, rank, note_id等字段

    Returns:
        bool: 操作是否成功
    """
    if not SUPABASE_AVAILABLE or not supabase_config.is_connected():
        if utils:
            utils.logger.warning("Supabase not available, skipping search_result insert")
        return False
    
    if not search_result_list:
        if utils:
            utils.logger.warning("Empty search_result_list, skipping insert")
        return True
    
    try:
        client = supabase_config.client
        
        # 准备批量数据
        data_list = []
        for search_item in search_result_list:
            data = {
                "keyword": search_item.get("keyword"),
                "search_account": search_item.get("search_account"),
                "rank": search_item.get("rank"),
                "note_id": search_item.get("note_id"),
            }
            data_list.append(data)
        
        # 批量插入搜索结果
        result = client.table("xhs_search_result").insert(data_list).execute()
        
        if utils:
            utils.logger.info(f"Successfully inserted {len(data_list)} search_results")
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