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
# @Time    : 2024/1/14 16:58
# @Desc    : 小红书存储实现类
import asyncio
import csv
import json
import os
import pathlib
from typing import Dict, List

import aiofiles
import config
from base.base_crawler import AbstractStore
from tools import utils, words
from var import crawler_type_var
import datetime 
import pytz  # 需要安装 pytz: pip install pytz

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def calculate_number_of_files(file_store_path: str) -> int:
    """计算数据保存文件的前部分排序数字，支持每次运行代码不写到同一个文件中
    Args:
        file_store_path;
    Returns:
        file nums
    """
    if not os.path.exists(file_store_path):
        return 1
    try:
        return max([int(file_name.split("_")[0])for file_name in os.listdir(file_store_path)])+1
    except ValueError:
        return 1


class XhsCsvStoreImplement(AbstractStore):
    csv_store_path: str = "data/xhs"
    file_count:int=calculate_number_of_files(csv_store_path)

    def make_save_file_name(self, store_type: str) -> str:
        """
        make save file name by store type
        Args:
            store_type: contents or comments

        Returns: eg: data/xhs/search_comments_20240114.csv ...

        """
        return f"{self.csv_store_path}/{self.file_count}_{crawler_type_var.get()}_{store_type}_{utils.get_current_date()}.csv"

    async def save_data_to_csv(self, save_item: Dict, store_type: str):
        """
        Below is a simple way to save it in CSV format.
        Args:
            save_item:  save content dict info
            store_type: Save type contains content and comments（contents | comments）

        Returns: no returns

        """
        pathlib.Path(self.csv_store_path).mkdir(parents=True, exist_ok=True)
        save_file_name = self.make_save_file_name(store_type=store_type)
        async with aiofiles.open(save_file_name, mode='a+', encoding="utf-8-sig", newline="") as f:
            f.fileno()
            writer = csv.writer(f)
            if await f.tell() == 0:
                await writer.writerow(save_item.keys())
            await writer.writerow(save_item.values())

    async def store_content(self, content_item: Dict):
        """
        Xiaohongshu content CSV storage implementation
        Args:
            content_item: note item dict

        Returns:

        """
        await self.save_data_to_csv(save_item=content_item, store_type="contents")

    async def store_comment(self, comment_item: Dict):
        """
        Xiaohongshu comment CSV storage implementation
        Args:
            comment_item: comment item dict

        Returns:

        """
        await self.save_data_to_csv(save_item=comment_item, store_type="comments")

    async def store_creator(self, creator: Dict):
        """
        Xiaohongshu content CSV storage implementation
        Args:
            creator: creator dict

        Returns:

        """
        await self.save_data_to_csv(save_item=creator, store_type="creator")

    async def store_search_result(self, search_item_list: List[Dict]):
        """
        搜索结果存储实现 - 保存到CSV文件
        Args:
            search_item_list: 搜索结果列表，每个item包含keyword, rank, note_id

        Returns:

        """
        await self.save_data_to_csv(save_item=search_item_list, store_type="search_result")

    # 使用示例
    async def convert_comments_to_conversations(self):
        """将评论数据转换为对话树并保存到单个JSONL文件"""

              # 实现你的逻辑
        print("Converting comments to conversations...")


class XhsDbStoreImplement(AbstractStore):
    async def store_content(self, content_item: Dict):
        """
        Xiaohongshu content DB storage implementation
        Args:
            content_item: content item dict

        Returns:

        """
        # format image urls,否则图片无法脱离xhs平台打开。 
        content_item['image_list'] = self.format_content_image_list(content_item.get('image_list'))
        # format last_update_time
        dt_utc = datetime.datetime.fromtimestamp(content_item['last_update_time']/1000).replace(tzinfo=datetime.timezone.utc)
        # 2. 转为东八区（北京时间）
        dt_east8 = dt_utc.astimezone(pytz.timezone('Asia/Shanghai'))
        content_item['last_update_time'] = dt_east8.isoformat()
        
        # 准备要upsert的数据，默认使用新数据
        new_content_item = content_item.copy()
        
        # 优先尝试保存到Supabase
        supabase_success = False
        try:
            from .xhs_store_sql import supa_upsert_note_detail, supa_query_note_by_id
            
            # 先查询数据库中是否已存在该记录
            note_id = content_item.get("note_id")
            old_content_item = await supa_query_note_by_id(note_id)
            
            if old_content_item:
                utils.logger.info(f"Found existing record for note_id: {note_id}, comparing timestamps...")
                
                # 比较时间戳 - 比较内容的last_update_time
                new_last_update = content_item.get('last_update_time')
                old_last_update = old_content_item.get('last_update_time')
                
                # 如果两个时间都存在，进行比较
                if old_last_update and new_last_update:
                    # 解析时间字符串进行比较
                    try:
                        # 处理新数据的时间（已经是ISO格式）
                        if isinstance(new_last_update, str):
                            # 处理不同的时间格式
                            new_time_str = new_last_update.replace('Z', '+00:00')
                            if 'T' not in new_time_str:
                                # 处理类似 "2023-03-28 15:25:36+00" 的格式
                                new_time_str = new_time_str.replace(' ', 'T')
                            new_time = datetime.datetime.fromisoformat(new_time_str)
                        else:
                            # 如果还是时间戳格式
                            new_time = datetime.datetime.fromtimestamp(new_last_update/1000).replace(tzinfo=datetime.timezone.utc)
                        
                        # 处理数据库中的时间
                        if isinstance(old_last_update, str):
                            # 处理不同的时间格式
                            old_time_str = old_last_update.replace('Z', '+00:00')
                            if 'T' not in old_time_str:
                                # 处理类似 "2023-03-28 15:25:36+00" 的格式
                                old_time_str = old_time_str.replace(' ', 'T')
                            old_time = datetime.datetime.fromisoformat(old_time_str)
                        else:
                            old_time = old_last_update
                        
                        # 如果新内容的更新时间不大于数据库记录的更新时间，保留AI分析字段
                        if new_time <= old_time:
                            utils.logger.info(f"Content not updated since last analysis (new: {new_time}, old: {old_time}), preserving AI fields for note_id: {note_id}")
                            
                            # 保留AI分析相关字段
                            ai_fields = ['brand_list', 'spu_list', 'emotion_dict', 'evaluation_dict']
                            for field in ai_fields:
                                if field in old_content_item and old_content_item[field]:
                                    new_content_item[field] = old_content_item[field]
                                    utils.logger.debug(f"Preserved AI field '{field}' from existing record")
                        else:
                            utils.logger.info(f"Content updated since last analysis (new: {new_time}, old: {old_time}), using new AI fields for note_id: {note_id}")
                    
                    except (ValueError, TypeError) as e:
                        utils.logger.warning(f"Error parsing timestamps for note_id {note_id}: {e}, proceeding with full update")
                else:
                    utils.logger.info(f"Missing timestamp information for note_id {note_id}, proceeding with full update")
                
                utils.logger.info(f"Updating existing record for note_id: {note_id}")
            else:
                utils.logger.info(f"Creating new record for note_id: {note_id}")
            
            # 执行upsert操作
            await supa_upsert_note_detail(new_content_item)
            supabase_success = True
            
        except Exception as e:
            utils.logger.warning(f"Failed to save content to Supabase: {e}")
        
        # 如果Supabase失败，尝试MySQL作为备用
        '''
        if not supabase_success:
            try:
                from .xhs_store_sql import (add_new_content,
                                            query_content_by_content_id,
                                            update_content_by_content_id)
                note_id = content_item.get("note_id")
                note_detail: Dict = await query_content_by_content_id(content_id=note_id)
                if not note_detail:
                    content_item["add_ts"] = utils.get_current_timestamp()
                    await add_new_content(content_item)
                else:
                    await update_content_by_content_id(note_id, content_item=content_item)
                utils.logger.info(f"Successfully saved content to MySQL as fallback: {note_id}")
            except Exception as e:
                utils.logger.error(f"Failed to save content to both Supabase and MySQL: {e}")
                raise
        '''
    async def store_comment(self, comment_item: Dict):
        """
        Xiaohongshu comment DB storage implementation
        Args:
            comment_item: comment item dict

        Returns:

        """
        # 优先尝试保存到Supabase
        supabase_success = False
        try:
            from .xhs_store_sql import supa_insert_comment_detail
            await supa_insert_comment_detail(comment_item)
            supabase_success = True
            utils.logger.info(f"Successfully saved comment to Supabase: {comment_item.get('comment_id')}")
        except Exception as e:
            utils.logger.warning(f"Failed to save comment to Supabase: {e}")
        
        # 如果Supabase失败，尝试MySQL作为备用
        if not supabase_success:
            try:
                from .xhs_store_sql import (add_new_comment,
                                            query_comment_by_comment_id,
                                            update_comment_by_comment_id)
                comment_id = comment_item.get("comment_id")
                comment_detail: Dict = await query_comment_by_comment_id(comment_id=comment_id)
                if not comment_detail:
                    comment_item["add_ts"] = utils.get_current_timestamp()
                    await add_new_comment(comment_item)
                else:
                    await update_comment_by_comment_id(comment_id, comment_item=comment_item)
                utils.logger.info(f"Successfully saved comment to MySQL as fallback: {comment_id}")
            except Exception as e:
                utils.logger.error(f"Failed to save comment to both Supabase and MySQL: {e}")
                raise

    async def store_creator(self, creator: Dict):
        """
        Xiaohongshu creator DB storage implementation
        Args:
            creator: creator dict

        Returns:

        """
        # 优先尝试保存到Supabase
        supabase_success = False
        try:
            from .xhs_store_sql import supa_insert_author_detail
            await supa_insert_author_detail(creator)
            supabase_success = True
            utils.logger.info(f"Successfully saved creator to Supabase: {creator.get('user_id')}")
        except Exception as e:
            utils.logger.warning(f"Failed to save creator to Supabase: {e}")
        
        # 如果Supabase失败，尝试MySQL作为备用
        if not supabase_success:
            try:
                from .xhs_store_sql import (add_new_creator, query_creator_by_user_id,
                                            update_creator_by_user_id)
                user_id = creator.get("user_id")
                user_detail: Dict = await query_creator_by_user_id(user_id)
                if not user_detail:
                    creator["add_ts"] = utils.get_current_timestamp()
                    await add_new_creator(creator)
                else:
                    await update_creator_by_user_id(user_id, creator)
                utils.logger.info(f"Successfully saved creator to MySQL as fallback: {user_id}")
            except Exception as e:
                utils.logger.error(f"Failed to save creator to both Supabase and MySQL: {e}")
                raise

    async def store_search_result(self, search_item_list: List[Dict]):
        """
        搜索结果存储实现 - 保存到Supabase数据库
        Args:
            search_item_list: 搜索结果列表，每个item包含keyword, rank, note_id

        Returns:

        """
        try:
            from .xhs_store_sql import supa_insert_search_result
            await supa_insert_search_result(search_item_list)
            utils.logger.info(f"Successfully stored search result: keyword: {search_item_list[0].get('keyword')} - content size: {len(search_item_list)}")
        except Exception as e:
            utils.logger.error(f"Failed to save search result to Supabase: {e}")

    # 使用示例
    async def convert_comments_to_conversations(self):
        """将评论数据转换为对话树并保存到单个JSONL文件"""

              # 实现你的逻辑
        print("Converting comments to conversations...")

    def format_content_image_list(self, image_list: List[str]):
        """
        格式化内容文件中的图片URL
        将形如 "@http://sns-webpic-qc.xhscdn.com/202504042337/568cb5c1362ab1078345424e8ef643a9/spectrum/1040g0k03120o8e1ohi004140m717516tkt5qoeo!nd_dft_wlteh_jpg_3"
        转换为 "@http://sns-img-hw.xhscdn.com/spectrum/1040g0k03120o8e1ohi004140m717516tkt5qoeo!nd_dft_wlteh_jpg_3"
        
        将形如 "@http://sns-webpic-qc.xhscdn.com/202504021249/c38b871008ab80ac90667fe6ae9a24b8/1040g008316h4pr2k1e4g4bu0b6c0fk1fclskbp0!nd_dft_wlteh_jpg_3"
        转换为 "@http://sns-img-hw.xhscdn.com/1040g008316h4pr2k1e4g4bu0b6c0fk1fclskbp0!nd_dft_wlteh_jpg_3"
        """
        import re

        # 正则表达式模式 - 处理含有spectrum的链接
        pattern_spectrum = r'(http://sns-webpic-qc\.xhscdn\.com/\d+/[a-f0-9]+/spectrum/)([a-zA-Z0-9!_.-]+)'
        # 正则表达式模式 - 处理不含spectrum的链接
        pattern_normal = r'(http://sns-webpic-qc\.xhscdn\.com/\d+/[a-f0-9]+/)([a-zA-Z0-9!_.-]+)'
            
        # 将逗号分隔的URL字符串拆分为列表
        img_urls = image_list.split(',')
        formatted_images = []
        
        for img_url in img_urls:
            img_url = img_url.strip()
            # 先尝试处理含有spectrum的链接
            new_url = re.sub(pattern_spectrum, r'http://sns-img-hw.xhscdn.com/spectrum/\2', img_url)
            # 如果URL没有变化，说明第一个模式没有匹配，尝试第二个模式
            if new_url == img_url:
                new_url = re.sub(pattern_normal, r'http://sns-img-hw.xhscdn.com/\2', img_url)
            
            formatted_images.append(new_url)
        
        # 将格式化后的URL列表重新以逗号连接
        new_image_list = ','.join(formatted_images)
        return new_image_list



class XhsJsonStoreImplement(AbstractStore):
    json_store_path: str = "data/xhs/json"
    words_store_path: str = "data/xhs/words"
    lock = asyncio.Lock()
    file_count:int=calculate_number_of_files(json_store_path)
    WordCloud = words.AsyncWordCloudGenerator()

    def make_save_file_name(self, store_type: str) -> (str,str): # type: ignore
        """
        make save file name by store type
        Args:
            store_type: Save type contains content and comments（contents | comments）

        Returns:

        """

        return (
            f"{self.json_store_path}/{crawler_type_var.get()}_{store_type}_{utils.get_current_date()}.json",
            f"{self.words_store_path}/{crawler_type_var.get()}_{store_type}_{utils.get_current_date()}"
        )

    def make_save_jsonl_file_name(self, store_type: str) -> str:
        """
        make save file name by store type
        Args:
            store_type: Save type
        Returns:

        """

        return  f"{self.json_store_path}/{crawler_type_var.get()}_{store_type}_{utils.get_current_date()}.jsonl"
    
    async def save_data_to_json(self, save_item: Dict, store_type: str):
        """
        Below is a simple way to save it in json format.
        Args:
            save_item: save content dict info
            store_type: Save type contains content and comments（contents | comments）

        Returns:

        """
        pathlib.Path(self.json_store_path).mkdir(parents=True, exist_ok=True)
        pathlib.Path(self.words_store_path).mkdir(parents=True, exist_ok=True)
        save_file_name,words_file_name_prefix = self.make_save_file_name(store_type=store_type)
        save_data = []

        async with self.lock:
            if os.path.exists(save_file_name):
                async with aiofiles.open(save_file_name, 'r', encoding='utf-8') as file:
                    save_data = json.loads(await file.read())

            save_data.append(save_item)
            async with aiofiles.open(save_file_name, 'w', encoding='utf-8') as file:
                await file.write(json.dumps(save_data, ensure_ascii=False, indent=4))

            if config.ENABLE_GET_COMMENTS and config.ENABLE_GET_WORDCLOUD:
                try:
                    await self.WordCloud.generate_word_frequency_and_cloud(save_data, words_file_name_prefix)
                except:
                    pass
    async def store_content(self, content_item: Dict):
        """
        content JSON storage implementation
        Args:
            content_item:

        Returns:

        """
        await self.save_data_to_json(content_item, "contents")

    async def store_comment(self, comment_item: Dict):
        """
        comment JSON storage implementatio
        Args:
            comment_item:

        Returns:

        """
        await self.save_data_to_json(comment_item, "comments")

    async def store_creator(self, creator: Dict):
        """
        Xiaohongshu content JSON storage implementation
        Args:
            creator: creator dict

        Returns:

        """
        await self.save_data_to_json(creator, "creator")
    
    async def store_search_result(self, search_item_list: List[Dict]):
        """
        搜索结果存储实现 - 保存到JSON文件
        Args:
            search_item_list: 搜索结果列表，每个item包含keyword, rank, note_id

        Returns:

        """
        await self.save_data_to_json(search_item_list, "search_result")

    async def build_comment_conversations_v2(self, input_file: str, output_dir: str = None) -> None:
        """
        从评论JSON文件构建对话, 列表结构并保存为单个JSONL文件
        支持多层级嵌套评论
        
        Args:
            input_file: 输入的评论JSON文件路径
            output_dir: 输出目录，默认为config.COMMENT_CORPUS_DIR
        """
        
        # 读取评论数据
        with open(input_file, 'r', encoding='utf-8') as f:
            comments = json.load(f)
        
        pathlib.Path(self.json_store_path).mkdir(parents=True, exist_ok=True)
        pathlib.Path(self.words_store_path).mkdir(parents=True, exist_ok=True)
        save_file_name = self.make_save_jsonl_file_name(store_type="comment_conversations")

        # 按笔记ID分组
        notes_comments = {}
        for comment in comments:
            note_id = comment.get('note_id')
            if not note_id:
                continue
            
            if note_id not in notes_comments:
                notes_comments[note_id] = []
            
            notes_comments[note_id].append(comment)
        
        # 所有对话的集合
        all_conversations = []
        
        # 处理每个笔记的评论
        for note_id, comments_list in notes_comments.items():
            # 创建评论ID到评论的映射
            comment_map = {comment.get('comment_id'): comment for comment in comments_list}
            
            # 构建完整的评论树结构
            comment_tree = {}
            for comment in comments_list:
                comment_id = comment.get('comment_id')
                parent_id = comment.get('parent_comment_id')
                
                # 初始化当前评论的子评论列表
                if comment_id not in comment_tree:
                    comment_tree[comment_id] = []
                
                # 如果是子评论，将其添加到父评论的子评论列表中
                if parent_id and parent_id != 0:
                    if parent_id not in comment_tree:
                        comment_tree[parent_id] = []
                    comment_tree[parent_id].append(comment_id)
            
            # 找出所有根评论（parent_comment_id为0的评论）
            root_comments = [comment for comment in comments_list if comment.get('parent_comment_id') == 0 or not comment.get('parent_comment_id')]
            
            # 递归构建评论树中的消息
            def build_messages(comment_id, depth=0, max_depth=config.COMMENT_CONVERSATION_MAX_DEPTH):
                if depth > max_depth:  # 防止无限递归
                    return []
                
                if comment_id not in comment_map:
                    return []
                
                comment = comment_map[comment_id]
                messages = [{
                    "comment_id": comment.get('comment_id'),
                    "user_id": comment.get('user_id'),
                    "user_name": comment.get('nickname'),
                    "content": comment.get('content'),
                    "pictures": comment.get('pictures', ''),
                    "create_time": comment.get('create_time'),
                    "is_author": comment.get('is_author', False),
                    "like_count": comment.get('like_count', 0),
                    "depth": depth  # 添加深度信息，方便调试
                }]
                
                # 添加所有子评论及其子评论
                if comment_id in comment_tree:
                    for child_id in comment_tree[comment_id]:
                        # 对每个直接子评论，递归获取它的所有子评论
                        child_messages = build_messages(child_id, depth + 1, max_depth)
                        messages.extend(child_messages)
                
                return messages
            
            # 对每个根评论，构建完整的对话树
            for root in root_comments:
                root_id = root.get('comment_id')
                # 只处理有回复的根评论，或者是作者的评论
                if (root_id in comment_tree and comment_tree[root_id]) or root.get('is_author', False):
                    # 构建此根评论的完整对话
                    conversation_messages = build_messages(root_id)
                    
                    # 只有至少有两条消息的对话才添加（确保有对话发生）
                    if len(conversation_messages) > 1:
                        conversation = {
                            "note_id": note_id,
                            "messages": conversation_messages
                        }
                        all_conversations.append(conversation)
        
        # 保存所有对话到一个JSONL文件
        if all_conversations:
            async with aiofiles.open(save_file_name, 'w', encoding='utf-8') as f:
                for conversation in all_conversations:
                    await f.write(json.dumps(conversation, ensure_ascii=False) + '\n')
            
            utils.logger.info(f"已将所有笔记的 {len(all_conversations)} 个对话保存到 {save_file_name}")
        else:
            utils.logger.warning(f"未找到有效对话，未生成文件")

    async def build_comment_conversations(self, input_file: str, output_dir: str = None) -> None:
        """
        从评论JSON文件构建对话，嵌套树状结构并保存为单个JSONL文件
        
        Args:
            input_file: 输入的评论JSON文件路径
            output_dir: 输出目录，默认为config.COMMENT_CORPUS_DIR
        """
        
        # 读取评论数据
        with open(input_file, 'r', encoding='utf-8') as f:
            comments = json.load(f)
        
        pathlib.Path(self.json_store_path).mkdir(parents=True, exist_ok=True)
        pathlib.Path(self.words_store_path).mkdir(parents=True, exist_ok=True)
        save_file_name = self.make_save_jsonl_file_name(store_type="comment_conversations")

        # 按笔记ID分组
        notes_comments = {}
        for comment in comments:
            note_id = comment.get('note_id')
            if not note_id:
                continue
            
            if note_id not in notes_comments:
                notes_comments[note_id] = []
            
            notes_comments[note_id].append(comment)
        
        # 所有对话的集合
        all_conversations = []
        
        # 处理每个笔记的评论
        for note_id, comments_list in notes_comments.items():
            # 创建评论ID到评论的映射
            comment_map = {comment.get('comment_id'): comment for comment in comments_list}
            
            # 构建父子关系映射
            child_to_parent = {}
            for comment in comments_list:
                comment_id = comment.get('comment_id')
                parent_id = comment.get('parent_comment_id')
                if parent_id and parent_id != 0:
                    child_to_parent[comment_id] = parent_id
            
            # 构建评论树结构
            comment_tree = {}
            for comment in comments_list:
                comment_id = comment.get('comment_id')
                parent_id = comment.get('parent_comment_id')
                
                # 初始化当前评论的子评论列表
                if comment_id not in comment_tree:
                    comment_tree[comment_id] = []
                
                # 如果是子评论，将其添加到父评论的子评论列表中
                if parent_id and parent_id != 0:
                    if parent_id not in comment_tree:
                        comment_tree[parent_id] = []
                    comment_tree[parent_id].append(comment_id)
            
            # 找出所有根评论（parent_comment_id为0的评论）
            root_comments = [comment for comment in comments_list if comment.get('parent_comment_id') == 0 or not comment.get('parent_comment_id')]
            
            # 递归构建评论树
            def build_comment_tree(comment_id, depth=0, max_depth=config.COMMENT_CONVERSATION_MAX_DEPTH):
                if depth > max_depth:  # 防止无限递归
                    return None
                
                if comment_id not in comment_map:
                    return None
                
                comment = comment_map[comment_id]
                
                # 构建评论节点
                comment_node = {
                    "comment_id": comment.get('comment_id'),
                    "user_id": comment.get('user_id'),
                    "user_name": comment.get('nickname'),
                    "content": comment.get('content'),
                    "pictures": comment.get('pictures', ''),
                    "create_time": comment.get('create_time'),
                    "is_author": comment.get('is_author', False),
                    "like_count": comment.get('like_count', 0),
                    "depth": depth,
                    "replies": []
                }
                
                # 添加子评论
                if comment_id in comment_tree:
                    for child_id in comment_tree[comment_id]:
                        child_node = build_comment_tree(child_id, depth + 1, max_depth)
                        if child_node:
                            comment_node["replies"].append(child_node)
                
                return comment_node
            
            # 对每个根评论构建树
            for root in root_comments:
                root_id = root.get('comment_id')
                # 只处理有回复的根评论，或者是作者的评论
                if (root_id in comment_tree and comment_tree[root_id]) or root.get('is_author', False):
                    # 构建此根评论的完整对话树
                    root_node = build_comment_tree(root_id)
                    
                    # 只有有回复的对话才添加
                    if root_node and (root_node["replies"] or root.get('is_author', False)):
                        conversation = {
                            "note_id": note_id,
                            "message": root_node
                        }
                        all_conversations.append(conversation)
        
        # 保存所有对话到一个JSONL文件
        if all_conversations:
            async with aiofiles.open(save_file_name, 'w', encoding='utf-8') as f:
                for conversation in all_conversations:
                    await f.write(json.dumps(conversation, ensure_ascii=False) + '\n')
            
            utils.logger.info(f"已将所有笔记的 {len(all_conversations)} 个对话树保存到 {save_file_name}")
        else:
            utils.logger.warning(f"未找到有效对话，未生成文件")

    async def build_comment_conversations_v3(self, input_file: str, output_dir: str = None) -> None:
        """
        从评论JSON文件构建扁平化的对话链结构并保存为单个JSONL文件
        每条从根到叶子的完整路径作为一个独立对话，适合向量检索
        
        Args:
            input_file: 输入的评论JSON文件路径
            output_dir: 输出目录，默认为config.COMMENT_CORPUS_DIR
        """
        import uuid
        
        # 读取评论数据
        with open(input_file, 'r', encoding='utf-8') as f:
            comments = json.load(f)
        
        pathlib.Path(self.json_store_path).mkdir(parents=True, exist_ok=True)
        pathlib.Path(self.words_store_path).mkdir(parents=True, exist_ok=True)
        save_file_name = self.make_save_jsonl_file_name(store_type="comment_conversations_flat")

        # 按笔记ID分组
        notes_comments = {}
        for comment in comments:
            note_id = comment.get('note_id')
            if not note_id:
                continue
            
            if note_id not in notes_comments:
                notes_comments[note_id] = []
            
            notes_comments[note_id].append(comment)
        
        # 所有扁平化对话链的集合
        all_conversation_chains = []
        
        # 处理每个笔记的评论
        for note_id, comments_list in notes_comments.items():
            # 创建评论ID到评论的映射
            comment_map = {comment.get('comment_id'): comment for comment in comments_list}
            
            # 构建评论树结构
            comment_tree = {}
            for comment in comments_list:
                comment_id = comment.get('comment_id')
                parent_id = comment.get('parent_comment_id')
                
                # 初始化当前评论的子评论列表
                if comment_id not in comment_tree:
                    comment_tree[comment_id] = []
                
                # 如果是子评论，将其添加到父评论的子评论列表中
                if parent_id and parent_id != 0:
                    if parent_id not in comment_tree:
                        comment_tree[parent_id] = []
                    comment_tree[parent_id].append(comment_id)
            
            # 找出所有根评论（parent_comment_id为0的评论）
            root_comments = [comment for comment in comments_list if comment.get('parent_comment_id') == 0 or not comment.get('parent_comment_id')]
            
            # DFS遍历构建所有从根到叶子的路径
            def build_comment_path(comment_id, path=None, depth=0, max_depth=config.COMMENT_CONVERSATION_MAX_DEPTH):
                if depth > max_depth:  # 防止无限递归
                    return []
                
                if comment_id not in comment_map:
                    return []
                
                if path is None:
                    path = []
                
                # 获取当前评论
                comment = comment_map[comment_id]
                
                # 构建当前评论节点
                comment_node = {
                    "note_id": note_id,
                    "conversation_id": str(uuid.uuid4()),  # 使用UUID生成唯一对话ID
                    "comment_id": comment.get('comment_id'),
                    "parent_id": comment.get('parent_comment_id') if comment.get('parent_comment_id') != 0 else None,
                    "user_id": comment.get('user_id'),
                    "user_name": comment.get('nickname'),
                    "content": comment.get('content'),
                    "pictures": comment.get('pictures', ''),
                    "create_time": comment.get('create_time'),
                    "depth": depth,
                    "like_count": comment.get('like_count', 0),
                    "is_author": comment.get('is_author', False)
                }
                
                # 复制当前路径并添加当前节点
                current_path = path + [comment_node]
                
                # 如果没有子评论，这是一条完整的路径
                if comment_id not in comment_tree or not comment_tree[comment_id]:
                    return [current_path]
                
                # 有子评论，继续DFS遍历
                all_paths = []
                for child_id in comment_tree[comment_id]:
                    child_paths = build_comment_path(
                        child_id, 
                        current_path,
                        depth + 1, 
                        max_depth
                    )
                    all_paths.extend(child_paths)
                
                # 如果没有任何子路径返回(可能是因为超过深度限制)，也将当前路径作为一条完整路径
                if not all_paths:
                    return [current_path]
                    
                return all_paths
            
            # 对每个根评论，构建所有可能的对话链
            for root in root_comments:
                root_id = root.get('comment_id')
                # 只处理有回复的根评论，或者是作者的评论
                if (root_id in comment_tree and comment_tree[root_id]) or root.get('is_author', False):
                    # 构建此根评论的所有对话链
                    comment_paths = build_comment_path(root_id)
                    
                    # 为每条链路分配唯一ID并保存
                    for path in comment_paths:
                        # 只保留有实际对话的路径(至少两条消息)
                        if len(path) >= 2:
                            # 为路径中的每个评论添加对话ID
                            for node in path:
                                node_with_conv_id = node.copy()
                                all_conversation_chains.append(node_with_conv_id)
        
        # 保存所有对话链到一个JSONL文件
        if all_conversation_chains:
            # 排序，确保同一对话的消息相邻且按深度排序
            all_conversation_chains.sort(key=lambda x: (x["conversation_id"], x["depth"]))
            
            async with aiofiles.open(save_file_name, 'w', encoding='utf-8') as f:
                for comment in all_conversation_chains:
                    await f.write(json.dumps(comment, ensure_ascii=False) + '\n')
            
            conversation_count = len(set(comment["conversation_id"] for comment in all_conversation_chains))
            message_count = len(all_conversation_chains)
            utils.logger.info(f"已将所有笔记的 {conversation_count} 条对话链（共 {message_count} 条消息）保存到 {save_file_name}")
        else:
            utils.logger.warning(f"未找到有效对话，未生成文件")

    async def build_comment_conversations_include_author(self, input_file: str, output_dir: str = None) -> None:
        """
        从评论JSON文件构建扁平化的对话链结构并保存为单个JSONL文件
        每条从根到叶子的完整路径作为一个独立对话，适合向量检索
        只保留包含作者回复(is_author=True)的对话
        
        Args:
            input_file: 输入的评论JSON文件路径
            output_dir: 输出目录，默认为config.COMMENT_CORPUS_DIR
        """
        import uuid
        
        # 读取评论数据
        with open(input_file, 'r', encoding='utf-8') as f:
            comments = json.load(f)
        
        pathlib.Path(self.json_store_path).mkdir(parents=True, exist_ok=True)
        pathlib.Path(self.words_store_path).mkdir(parents=True, exist_ok=True)
        save_file_name = self.make_save_jsonl_file_name(store_type="comment_conversations_author_flat")

        # 按笔记ID分组
        notes_comments = {}
        for comment in comments:
            note_id = comment.get('note_id')
            if not note_id:
                continue
            
            if note_id not in notes_comments:
                notes_comments[note_id] = []
            
            notes_comments[note_id].append(comment)
        
        # 所有扁平化对话链的集合
        all_conversation_chains = []
        
        # 处理每个笔记的评论
        for note_id, comments_list in notes_comments.items():
            # 创建评论ID到评论的映射
            comment_map = {comment.get('comment_id'): comment for comment in comments_list}
            
            # 构建评论树结构
            comment_tree = {}
            for comment in comments_list:
                comment_id = comment.get('comment_id')
                parent_id = comment.get('parent_comment_id')
                
                # 初始化当前评论的子评论列表
                if comment_id not in comment_tree:
                    comment_tree[comment_id] = []
                
                # 如果是子评论，将其添加到父评论的子评论列表中
                if parent_id and parent_id != 0:
                    if parent_id not in comment_tree:
                        comment_tree[parent_id] = []
                    comment_tree[parent_id].append(comment_id)
            
            # 找出所有根评论（parent_comment_id为0的评论）
            root_comments = [comment for comment in comments_list if comment.get('parent_comment_id') == 0 or not comment.get('parent_comment_id')]
            
            # DFS遍历构建所有从根到叶子的路径
            def build_comment_path(comment_id, path=None, depth=0, max_depth=config.COMMENT_CONVERSATION_MAX_DEPTH):
                if depth > max_depth:  # 防止无限递归
                    return []
                
                if comment_id not in comment_map:
                    return []
                
                if path is None:
                    path = []
                
                # 获取当前评论
                comment = comment_map[comment_id]
                
                # 构建当前评论节点
                comment_node = {
                    "note_id": note_id,
                    "conversation_id": str(uuid.uuid4()),  # 使用UUID生成唯一对话ID
                    "comment_id": comment.get('comment_id'),
                    "parent_id": comment.get('parent_comment_id') if comment.get('parent_comment_id') != 0 else None,
                    "user_id": comment.get('user_id'),
                    "user_name": comment.get('nickname'),
                    "content": comment.get('content'),
                    "pictures": comment.get('pictures', ''),
                    "create_time": comment.get('create_time'),
                    "depth": depth,
                    "like_count": comment.get('like_count', 0),
                    "is_author": comment.get('is_author', False)
                }
                
                # 复制当前路径并添加当前节点
                current_path = path + [comment_node]
                
                # 如果没有子评论，这是一条完整的路径
                if comment_id not in comment_tree or not comment_tree[comment_id]:
                    return [current_path]
                
                # 有子评论，继续DFS遍历
                all_paths = []
                for child_id in comment_tree[comment_id]:
                    child_paths = build_comment_path(
                        child_id, 
                        current_path,
                        depth + 1, 
                        max_depth
                    )
                    all_paths.extend(child_paths)
                
                # 如果没有任何子路径返回(可能是因为超过深度限制)，也将当前路径作为一条完整路径
                if not all_paths:
                    return [current_path]
                    
                return all_paths
            
            # 对每个根评论，构建所有可能的对话链
            for root in root_comments:
                root_id = root.get('comment_id')
                # 只处理有回复的根评论，或者是作者的评论
                if (root_id in comment_tree and comment_tree[root_id]) or root.get('is_author', False):
                    # 构建此根评论的所有对话链
                    comment_paths = build_comment_path(root_id)
                    
                    # 为每条链路分配唯一ID并保存
                    for path in comment_paths:
                        # 只保留有实际对话的路径(至少两条消息)
                        if len(path) >= 2:
                            # 为路径中的每个评论添加对话ID
                            conversation_id = str(uuid.uuid4())
                            for node in path:
                                node_with_conv_id = node.copy()
                                node_with_conv_id["conversation_id"] = conversation_id
                                all_conversation_chains.append(node_with_conv_id)
        
        # 保存所有对话链到一个JSONL文件
        if all_conversation_chains:
            # 按对话ID分组，仅保留包含作者评论的对话
            conversation_ids = set()
            conversations_with_author = {}
            
            # 找出所有包含作者评论的conversation_id
            for comment in all_conversation_chains:
                conv_id = comment["conversation_id"]
                if conv_id not in conversations_with_author:
                    conversations_with_author[conv_id] = []
                
                conversations_with_author[conv_id].append(comment)
                
                if comment.get("is_author", False):
                    conversation_ids.add(conv_id)
            
            # 只保留包含作者评论的对话
            filtered_chains = []
            for conv_id in conversation_ids:
                filtered_chains.extend(conversations_with_author[conv_id])
            
            # 排序，确保同一对话的消息相邻且按深度排序
            filtered_chains.sort(key=lambda x: (x["conversation_id"], x["depth"]))
            
            async with aiofiles.open(save_file_name, 'w', encoding='utf-8') as f:
                for comment in filtered_chains:
                    await f.write(json.dumps(comment, ensure_ascii=False) + '\n')
            
            conversation_count = len(conversation_ids)
            message_count = len(filtered_chains)
            total_conversations = len(conversations_with_author)
            utils.logger.info(f"已将所有笔记的 {conversation_count}/{total_conversations} 条包含作者回复的对话链（共 {message_count} 条消息）保存到 {save_file_name}")
        else:
            utils.logger.warning(f"未找到有效对话，未生成文件")


    async def format_content_image_urls(self):
        """
        格式化内容文件中的图片URL
        将形如 "@http://sns-webpic-qc.xhscdn.com/202504042337/568cb5c1362ab1078345424e8ef643a9/spectrum/1040g0k03120o8e1ohi004140m717516tkt5qoeo!nd_dft_wlteh_jpg_3"
        转换为 "@http://sns-img-hw.xhscdn.com/spectrum/1040g0k03120o8e1ohi004140m717516tkt5qoeo!nd_dft_wlteh_jpg_3"
        
        将形如 "@http://sns-webpic-qc.xhscdn.com/202504021249/c38b871008ab80ac90667fe6ae9a24b8/1040g008316h4pr2k1e4g4bu0b6c0fk1fclskbp0!nd_dft_wlteh_jpg_3"
        转换为 "@http://sns-img-hw.xhscdn.com/1040g008316h4pr2k1e4g4bu0b6c0fk1fclskbp0!nd_dft_wlteh_jpg_3"
        """
        import glob
        import re
        import os
        import json
        import aiofiles
        
        # 确保输出目录存在
        output_dir = os.path.join(self.json_store_path, "formatted")
        os.makedirs(output_dir, exist_ok=True)
        
        # 获取所有内容文件
        content_files = glob.glob(os.path.join(self.json_store_path, "creator_contents_*.json"))
        
        if not content_files:
            utils.logger.warning("未找到内容文件，无法格式化图片URL")
            return
        
        # 合并后的输出文件
        output_file = os.path.join(output_dir, "formatted_contents.json")
        
        # 正则表达式模式 - 处理含有spectrum的链接
        pattern_spectrum = r'(http://sns-webpic-qc\.xhscdn\.com/\d+/[a-f0-9]+/spectrum/)([a-zA-Z0-9!_.-]+)'
        # 正则表达式模式 - 处理不含spectrum的链接
        pattern_normal = r'(http://sns-webpic-qc\.xhscdn\.com/\d+/[a-f0-9]+/)([a-zA-Z0-9!_.-]+)'
        
        all_formatted_data = []
        
        for file_path in content_files:
            utils.logger.info(f"处理内容文件: {file_path}")
            
            try:
                async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
                    content = await f.read()
                    data = json.loads(content)
                    
                    for item in data:
                        if "image_list" in item and isinstance(item["image_list"], str) and item["image_list"]:
                            # 将逗号分隔的URL字符串拆分为列表
                            img_urls = item["image_list"].split(',')
                            formatted_images = []
                            
                            for img_url in img_urls:
                                img_url = img_url.strip()
                                # 先尝试处理含有spectrum的链接
                                new_url = re.sub(pattern_spectrum, r'http://sns-img-hw.xhscdn.com/spectrum/\2', img_url)
                                # 如果URL没有变化，说明第一个模式没有匹配，尝试第二个模式
                                if new_url == img_url:
                                    new_url = re.sub(pattern_normal, r'http://sns-img-hw.xhscdn.com/\2', img_url)
                                
                                formatted_images.append(new_url)
                            
                            # 将格式化后的URL列表重新以逗号连接
                            item["image_list"] = ','.join(formatted_images)
                        
                        all_formatted_data.append(item)
            except Exception as e:
                utils.logger.error(f"处理文件 {file_path} 时出错: {e}")
        
        # 保存合并后的格式化数据
        try:
            async with aiofiles.open(output_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(all_formatted_data, ensure_ascii=False, indent=2))
            utils.logger.info(f"已将格式化后的内容保存到: {output_file}")
        except Exception as e:
            utils.logger.error(f"保存格式化内容时出错: {e}")

    async def format_comment_image_urls(self):
        """
        格式化评论文件中的图片URL
        将形如 "@http://sns-webpic-qc.xhscdn.com/202504050002/7b550abb5a3826d2948cfc341ceaaad1/comment/1040g2h0310hh1p2p6k6g5nst4v408e7gvftabd0!nd_whgt34_webp_wm_1"
        转换为 "@http://sns-img-hw.xhscdn.com/comment/1040g2h0310hh1p2p6k6g5nst4v408e7gvftabd0!nd_whgt34_webp_wm_1"
        """
        import glob
        import re
        import os
        import json
        import aiofiles
        
        # 确保输出目录存在
        output_dir = os.path.join(self.json_store_path, "formatted")
        os.makedirs(output_dir, exist_ok=True)
        
        # 获取所有评论文件
        comment_files = glob.glob(os.path.join(self.json_store_path, "creator_comments_*.json"))
        
        if not comment_files:
            utils.logger.warning("未找到评论文件，无法格式化图片URL")
            return
        
        # 合并后的输出文件
        output_file = os.path.join(output_dir, "formatted_comments.json")
        
        # 正则表达式模式
        pattern = r'(http://sns-webpic-qc\.xhscdn\.com/\d+/[a-f0-9]+/comment/)([a-zA-Z0-9!_.-]+)'
        
        all_formatted_data = []
        
        for file_path in comment_files:
            utils.logger.info(f"处理评论文件: {file_path}")
            
            try:
                async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
                    content = await f.read()
                    data = json.loads(content)
                    
                    for item in data:
                        # 处理主评论中的图片
                        if "pictures" in item and isinstance(item["pictures"], str) and item["pictures"]:
                            # 这里pictures是一个单一的URL字符串，直接替换
                            item["pictures"] = re.sub(pattern, r'http://sns-img-hw.xhscdn.com/comment/\2', item["pictures"])
                        
                   
                        
                        all_formatted_data.append(item)
            except Exception as e:
                utils.logger.error(f"处理文件 {file_path} 时出错: {e}")
        
        # 保存合并后的格式化数据
        try:
            async with aiofiles.open(output_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(all_formatted_data, ensure_ascii=False, indent=2))
            utils.logger.info(f"已将格式化后的评论保存到: {output_file}")
        except Exception as e:
            utils.logger.error(f"保存格式化评论时出错: {e}")

    # 使用示例
    async def convert_comments_to_conversations(self):
        """将评论数据转换为对话树并保存到单个JSONL文件"""

        # 格式化图片URL
        await self.format_content_image_urls()
        await self.format_comment_image_urls()

        #
        output_dir = os.path.join(self.json_store_path, "formatted")
        input_file = os.path.join(output_dir, "formatted_comments.json")
        
        # test
        # input_file = "data/xhs/json/creator_comments_2025-04-05.json"
        
        if os.path.exists(input_file):
            await self.build_comment_conversations_include_author(input_file)
            utils.logger.info(f"已完成评论转换为对话树: {input_file}")
        else:
            utils.logger.error(f"评论文件不存在: {input_file}")


if __name__ == "__main__":
    asyncio.run(XhsCsvStoreImplement().convert_comments_to_conversations())
