# 声明：本代码仅供学习和研究目的使用。使用者应遵守以下原则：
# 1. 不得用于任何商业用途。
# 2. 使用时应遵守目标平台的使用条款和robots.txt规则。
# 3. 不得进行大规模爬取或对平台造成运营干扰。
# 4. 应合理控制请求频率，避免给目标平台带来不必要的负担。
# 5. 不得用于任何非法或不当的用途。
#
# 详细许可条款请参阅项目根目录下的LICENSE文件。
# 使用本代码即表示您同意遵守上述原则和LICENSE中的所有条款。


import asyncio
import os
import random
import time
from asyncio import Task
from typing import Dict, List, Optional, Tuple
import json
import aiofiles

from playwright.async_api import BrowserContext, BrowserType, Page, async_playwright
from tenacity import RetryError

import config
from base.base_crawler import AbstractCrawler
from config import CRAWLER_MAX_COMMENTS_COUNT_SINGLENOTES
from model.m_xiaohongshu import NoteUrlInfo
from proxy.proxy_ip_pool import IpInfoModel, create_ip_pool
from store import xhs as xhs_store
from tools import utils
from var import crawler_type_var, source_keyword_var

from .client import XiaoHongShuClient
from .exception import DataFetchError
from .field import SearchSortType
from .help import parse_note_info_from_note_url, get_search_id
from .login import XiaoHongShuLogin


class XiaoHongShuCrawler(AbstractCrawler):
    context_page: Page
    xhs_client: XiaoHongShuClient
    browser_context: BrowserContext

    def __init__(self) -> None:
        self.index_url = "https://www.xiaohongshu.com"
        # self.user_agent = utils.get_user_agent()
        self.user_agent = config.UA if config.UA else "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
        # 创建并保存 store 实例
        from store.xhs import XhsStoreFactory
        self.store = XhsStoreFactory.create_store()

    async def start(self) -> None:
        playwright_proxy_format, httpx_proxy_format = None, None
        if config.ENABLE_IP_PROXY:
            ip_proxy_pool = await create_ip_pool(
                config.IP_PROXY_POOL_COUNT, enable_validate_ip=True
            )
            ip_proxy_info: IpInfoModel = await ip_proxy_pool.get_proxy()
            playwright_proxy_format, httpx_proxy_format = self.format_proxy_info(
                ip_proxy_info
            )

        async with async_playwright() as playwright:
            # Launch a browser context.
            chromium = playwright.chromium
            self.browser_context = await self.launch_browser(
                chromium, None, self.user_agent, headless=config.HEADLESS
            )
            # stealth.min.js is a js script to prevent the website from detecting the crawler.
            await self.browser_context.add_init_script(path="libs/stealth.min.js")
            # add a cookie attribute webId to avoid the appearance of a sliding captcha on the webpage
            await self.browser_context.add_cookies(
                [
                    {
                        "name": "webId",
                        "value": "xxx123",  # any value
                        "domain": ".xiaohongshu.com",
                        "path": "/",
                    }
                ]
            )
            self.context_page = await self.browser_context.new_page()
            await self.context_page.goto(self.index_url)

            # Create a client to interact with the xiaohongshu website.
            self.xhs_client = await self.create_xhs_client(httpx_proxy_format)
            if not await self.xhs_client.pong():
                login_obj = XiaoHongShuLogin(
                    login_type=config.LOGIN_TYPE,
                    login_phone="",  # input your phone number
                    browser_context=self.browser_context,
                    context_page=self.context_page,
                    cookie_str=config.COOKIES,
                )
                await login_obj.begin()
                await self.xhs_client.update_cookies(
                    browser_context=self.browser_context
                )

            crawler_type_var.set(config.CRAWLER_TYPE)
            if config.CRAWLER_TYPE == "search":
                # Search for notes and retrieve their comment information.
                await self.search()
            elif config.CRAWLER_TYPE == "detail":
                # Get the information and comments of the specified post
                await self.get_specified_notes()
            elif config.CRAWLER_TYPE == "creator":
                # Get creator's information and their notes and comments
                await self.get_creators_and_notes()
            else:
                pass

            utils.logger.info("[XiaoHongShuCrawler.start] Xhs Crawler finished ...")

    async def search(self) -> None:
        """Search for notes and retrieve their comment information."""
        utils.logger.info(
            "[XiaoHongShuCrawler.search] Begin search xiaohongshu keywords"
        )
        xhs_limit_count = 20  # xhs limit page fixed value
        if config.CRAWLER_MAX_NOTES_COUNT < xhs_limit_count:
            config.CRAWLER_MAX_NOTES_COUNT = xhs_limit_count
        start_page = config.START_PAGE
        for keyword in config.KEYWORDS.split(","):
            source_keyword_var.set(keyword)
            utils.logger.info(
                f"[XiaoHongShuCrawler.search] Current search keyword: {keyword}"
            )
            page = 1
            search_id = get_search_id()
            while (
                page - start_page + 1
            ) * xhs_limit_count <= config.CRAWLER_MAX_NOTES_COUNT:
                if page < start_page:
                    utils.logger.info(f"[XiaoHongShuCrawler.search] Skip page {page}")
                    page += 1
                    continue

                try:
                    utils.logger.info(
                        f"[XiaoHongShuCrawler.search] search xhs keyword: {keyword}, page: {page}"
                    )
                    note_ids: List[str] = []
                    xsec_tokens: List[str] = []
                    notes_res = await self.xhs_client.get_note_by_keyword(
                        keyword=keyword,
                        search_id=search_id,
                        page=page,
                        sort=(
                            SearchSortType(config.SORT_TYPE)
                            if config.SORT_TYPE != ""
                            else SearchSortType.GENERAL
                        ),
                    )
                    utils.logger.info(
                        f"[XiaoHongShuCrawler.search] Search notes res:{notes_res}"
                    )
                    if not notes_res or not notes_res.get("has_more", False):
                        utils.logger.info("No more content!")
                        break
                    semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
                    task_list = [
                        self.get_note_detail_async_task(
                            note_id=post_item.get("id"),
                            xsec_source=post_item.get("xsec_source"),
                            xsec_token=post_item.get("xsec_token"),
                            semaphore=semaphore,
                        )
                        for post_item in notes_res.get("items", {})
                        if post_item.get("model_type") not in ("rec_query", "hot_query")
                    ]
                    note_details = await asyncio.gather(*task_list)
                    for note_detail in note_details:
                        if note_detail:
                            await xhs_store.update_xhs_note(note_detail)
                            await self.get_notice_media(note_detail)
                            note_ids.append(note_detail.get("note_id"))
                            xsec_tokens.append(note_detail.get("xsec_token"))
                    page += 1
                    utils.logger.info(
                        f"[XiaoHongShuCrawler.search] Note details: {note_details}"
                    )
                    await self.batch_get_note_comments(note_ids, xsec_tokens)
                except DataFetchError:
                    utils.logger.error(
                        "[XiaoHongShuCrawler.search] Get note detail error"
                    )
                    break

    async def get_creators_and_notes(self) -> None:
        """Get creator's notes and retrieve their comment information."""
        utils.logger.info(
            "[XiaoHongShuCrawler.get_creators_and_notes] Begin get xiaohongshu creators"
        )
        for user_id in config.XHS_CREATOR_ID_LIST:
            # get creator detail info from web html content
            createor_info: Dict = await self.xhs_client.get_creator_info(
                user_id=user_id
            )
            if createor_info:
                await xhs_store.save_creator(user_id, creator=createor_info)

            # When proxy is not enabled, increase the crawling interval
            if config.ENABLE_IP_PROXY:
                crawl_interval = random.random()
            else:
                crawl_interval = random.uniform(1, config.CRAWLER_MAX_SLEEP_SEC)
            # Get all note information of the creator
            all_notes_list = await self.xhs_client.get_all_notes_by_creator(
                user_id=user_id,
                crawl_interval=crawl_interval,
                callback=self.fetch_creator_notes_detail,
            )

            note_ids = []
            xsec_tokens = []
            for note_item in all_notes_list:
                note_ids.append(note_item.get("note_id"))
                xsec_tokens.append(note_item.get("xsec_token"))
            await self.batch_get_note_comments(note_ids, xsec_tokens)

    async def fetch_creator_notes_detail(self, note_list: List[Dict]):
        """
        Concurrently obtain the specified post list and save the data
        """
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        task_list = [
            self.get_note_detail_async_task(
                note_id=post_item.get("note_id"),
                xsec_source=post_item.get("xsec_source"),
                xsec_token=post_item.get("xsec_token"),
                semaphore=semaphore,
            )
            for post_item in note_list
        ]

        note_details = await asyncio.gather(*task_list)
        for note_detail in note_details:
            if note_detail:
                await xhs_store.update_xhs_note(note_detail)

    async def get_specified_notes(self):
        """
        Get the information and comments of the specified post
        must be specified note_id, xsec_source, xsec_token⚠️⚠️⚠️
        Returns:

        """
        get_note_detail_task_list = []
        for full_note_url in config.XHS_SPECIFIED_NOTE_URL_LIST:
            note_url_info: NoteUrlInfo = parse_note_info_from_note_url(full_note_url)
            utils.logger.info(
                f"[XiaoHongShuCrawler.get_specified_notes] Parse note url info: {note_url_info}"
            )
            crawler_task = self.get_note_detail_async_task(
                note_id=note_url_info.note_id,
                xsec_source=note_url_info.xsec_source,
                xsec_token=note_url_info.xsec_token,
                semaphore=asyncio.Semaphore(config.MAX_CONCURRENCY_NUM),
            )
            get_note_detail_task_list.append(crawler_task)

        need_get_comment_note_ids = []
        xsec_tokens = []
        note_details = await asyncio.gather(*get_note_detail_task_list)
        for note_detail in note_details:
            if note_detail:
                need_get_comment_note_ids.append(note_detail.get("note_id", ""))
                xsec_tokens.append(note_detail.get("xsec_token", ""))
                await xhs_store.update_xhs_note(note_detail)
        await self.batch_get_note_comments(need_get_comment_note_ids, xsec_tokens)

    async def get_note_detail_async_task(
        self,
        note_id: str,
        xsec_source: str,
        xsec_token: str,
        semaphore: asyncio.Semaphore,
    ) -> Optional[Dict]:
        """Get note detail

        Args:
            note_id:
            xsec_source:
            xsec_token:
            semaphore:

        Returns:
            Dict: note detail
        """
        note_detail_from_html, note_detail_from_api = None, None
        async with semaphore:
            # When proxy is not enabled, increase the crawling interval
            if config.ENABLE_IP_PROXY:
                crawl_interval = random.random()
            else:
                crawl_interval = random.uniform(1, config.CRAWLER_MAX_SLEEP_SEC)
            try:
                # 尝试直接获取网页版笔记详情，携带cookie
                note_detail_from_html: Optional[Dict] = (
                    await self.xhs_client.get_note_by_id_from_html(
                        note_id, xsec_source, xsec_token, enable_cookie=True
                    )
                )
                time.sleep(crawl_interval)
                if not note_detail_from_html:
                    # 如果网页版笔记详情获取失败，则尝试不使用cookie获取
                    note_detail_from_html = (
                        await self.xhs_client.get_note_by_id_from_html(
                            note_id, xsec_source, xsec_token, enable_cookie=False
                        )
                    )
                    utils.logger.error(
                        f"[XiaoHongShuCrawler.get_note_detail_async_task] Get note detail error, note_id: {note_id}"
                    )
                if not note_detail_from_html:
                    # 如果网页版笔记详情获取失败，则尝试API获取
                    note_detail_from_api: Optional[Dict] = (
                        await self.xhs_client.get_note_by_id(
                            note_id, xsec_source, xsec_token
                        )
                    )
                note_detail = note_detail_from_html or note_detail_from_api
                if note_detail:
                    note_detail.update(
                        {"xsec_token": xsec_token, "xsec_source": xsec_source}
                    )

                    # 保存笔记详情到JSONL，供后续处理
                    if config.ENABLE_COMMENT_CONVERSATION:
                        await self.save_note_detail_to_jsonl(note_id, note_detail)

                    return note_detail
            except DataFetchError as ex:
                utils.logger.error(
                    f"[XiaoHongShuCrawler.get_note_detail_async_task] Get note detail error: {ex}"
                )
                return None
            except KeyError as ex:
                utils.logger.error(
                    f"[XiaoHongShuCrawler.get_note_detail_async_task] have not fund note detail note_id:{note_id}, err: {ex}"
                )
                return None

    async def batch_get_note_comments(
        self, note_list: List[str], xsec_tokens: List[str]
    ):
        """Batch get note comments"""
        if not config.ENABLE_GET_COMMENTS:
            utils.logger.info(
                f"[XiaoHongShuCrawler.batch_get_note_comments] Crawling comment mode is not enabled"
            )
            return

        utils.logger.info(
            f"[XiaoHongShuCrawler.batch_get_note_comments] Begin batch get note comments, note list: {note_list}"
        )
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        task_list: List[Task] = []
        for index, note_id in enumerate(note_list):
            task = asyncio.create_task(
                self.get_comments(
                    note_id=note_id, xsec_token=xsec_tokens[index], semaphore=semaphore
                ),
                name=note_id,
            )
            task_list.append(task)
        await asyncio.gather(*task_list)

    @staticmethod
    def format_proxy_info(
        ip_proxy_info: IpInfoModel,
    ) -> Tuple[Optional[Dict], Optional[Dict]]:
        """format proxy info for playwright and httpx"""
        playwright_proxy = {
            "server": f"{ip_proxy_info.protocol}{ip_proxy_info.ip}:{ip_proxy_info.port}",
            "username": ip_proxy_info.user,
            "password": ip_proxy_info.password,
        }
        httpx_proxy = {
            f"{ip_proxy_info.protocol}": f"http://{ip_proxy_info.user}:{ip_proxy_info.password}@{ip_proxy_info.ip}:{ip_proxy_info.port}"
        }
        return playwright_proxy, httpx_proxy

    async def create_xhs_client(self, httpx_proxy: Optional[str]) -> XiaoHongShuClient:
        """Create xhs client"""
        utils.logger.info(
            "[XiaoHongShuCrawler.create_xhs_client] Begin create xiaohongshu API client ..."
        )
        cookie_str, cookie_dict = utils.convert_cookies(
            await self.browser_context.cookies()
        )
        xhs_client_obj = XiaoHongShuClient(
            proxies=httpx_proxy,
            headers={
                "User-Agent": self.user_agent,
                "Cookie": cookie_str,
                "Origin": "https://www.xiaohongshu.com",
                "Referer": "https://www.xiaohongshu.com",
                "Content-Type": "application/json;charset=UTF-8",
            },
            playwright_page=self.context_page,
            cookie_dict=cookie_dict,
        )
        return xhs_client_obj

    async def launch_browser(
        self,
        chromium: BrowserType,
        playwright_proxy: Optional[Dict],
        user_agent: Optional[str],
        headless: bool = True,
    ) -> BrowserContext:
        """Launch browser and create browser context"""
        utils.logger.info(
            "[XiaoHongShuCrawler.launch_browser] Begin create browser context ..."
        )
        if config.SAVE_LOGIN_STATE:
            # feat issue #14
            # we will save login state to avoid login every time
            user_data_dir = os.path.join(
                os.getcwd(), "browser_data", config.USER_DATA_DIR % config.PLATFORM
            )  # type: ignore
            browser_context = await chromium.launch_persistent_context(
                user_data_dir=user_data_dir,
                accept_downloads=True,
                headless=headless,
                proxy=playwright_proxy,  # type: ignore
                viewport={"width": 1920, "height": 1080},
                user_agent=user_agent,
            )
            return browser_context
        else:
            browser = await chromium.launch(headless=headless, proxy=playwright_proxy)  # type: ignore
            browser_context = await browser.new_context(
                viewport={"width": 1920, "height": 1080}, user_agent=user_agent
            )
            return browser_context

    async def close(self):
        """Close browser context"""
        await self.browser_context.close()
        utils.logger.info("[XiaoHongShuCrawler.close] Browser context closed ...")

    async def get_notice_media(self, note_detail: Dict):
        if not config.ENABLE_GET_IMAGES:
            utils.logger.info(
                f"[XiaoHongShuCrawler.get_notice_media] Crawling image mode is not enabled"
            )
            return
        await self.get_note_images(note_detail)
        await self.get_notice_video(note_detail)

    async def get_note_images(self, note_item: Dict):
        """
        get note images. please use get_notice_media
        :param note_item:
        :return:
        """
        if not config.ENABLE_GET_IMAGES:
            return
        note_id = note_item.get("note_id")
        image_list: List[Dict] = note_item.get("image_list", [])

        for img in image_list:
            if img.get("url_default") != "":
                img.update({"url": img.get("url_default")})

        if not image_list:
            return
        picNum = 0
        for pic in image_list:
            url = pic.get("url")
            if not url:
                continue
            content = await self.xhs_client.get_note_media(url)
            if content is None:
                continue
            extension_file_name = f"{picNum}.jpg"
            picNum += 1
            await xhs_store.update_xhs_note_image(note_id, content, extension_file_name)

    async def get_notice_video(self, note_item: Dict):
        """
        get note images. please use get_notice_media
        :param note_item:
        :return:
        """
        if not config.ENABLE_GET_IMAGES:
            return
        note_id = note_item.get("note_id")

        videos = xhs_store.get_video_url_arr(note_item)

        if not videos:
            return
        videoNum = 0
        for url in videos:
            content = await self.xhs_client.get_note_media(url)
            if content is None:
                continue
            extension_file_name = f"{videoNum}.mp4"
            videoNum += 1
            await xhs_store.update_xhs_note_image(note_id, content, extension_file_name)

    async def process_note_comments_to_jsonl(self, note_id: str, note_detail: Dict, comments: List[Dict]):
        """处理笔记评论并导出为JSONL格式
        
        Args:
            note_id: 笔记ID
            note_detail: 笔记详情
            comments: 评论列表
        """
        if not comments:
            utils.logger.info(f"[XiaoHongShuCrawler.process_note_comments_to_jsonl] No comments for note {note_id}")
            return
        
        # 创建输出目录
        output_dir = config.COMMENT_CORPUS_DIR
        os.makedirs(output_dir, exist_ok=True)
        
        # 获取笔记作者昵称
        author_nickname = note_detail.get("user", {}).get("nickname", "")
        note_title = note_detail.get("title", "")
        
        # 标记作者身份
        for comment in comments:
            comment["is_author"] = comment.get("user_info", {}).get("nickname") == author_nickname
            for sub_comment in comment.get("sub_comments", []):
                sub_comment["is_author"] = sub_comment.get("user_info", {}).get("nickname") == author_nickname
        
        # 构建对话树
        conversations = self._build_comment_conversations(note_id, note_detail, comments)
        
        # 保存为JSONL
        output_file = os.path.join(output_dir, f"{note_id}_{note_title}_{author_nickname}.jsonl")
        async with aiofiles.open(output_file, 'w', encoding='utf-8') as f:
            for conv in conversations:
                await f.write(json.dumps(conv, ensure_ascii=False) + '\n')
            
        utils.logger.info(f"[XiaoHongShuCrawler.process_note_comments_to_jsonl] Saved {len(conversations)} conversations for note {note_id}")

    def _build_comment_conversations(self, note_id: str, note_detail: Dict, comments: List[Dict]) -> List[Dict]:
        """构建评论对话树
        
        Args:
            note_id: 笔记ID
            note_detail: 笔记详情
            comments: 评论列表
            
        Returns:
            对话列表
        """
        # 过滤掉没有子评论的单条评论
        filtered_comments = [
            comment for comment in comments
            if not (
                (comment.get("sub_comment_count") == "0" or comment.get("sub_comment_count") == 0) and
                (comment.get("target_comment", {}).get("id", 0) == 0)
            )
        ]

        # test
        print("filtered_comments:", filtered_comments)
        
        conversations = []
        
        for comment in filtered_comments:
            conversation = {
                "note_id": note_id,
                "note_title": note_detail.get("title", ""),
                "note_desc": note_detail.get("desc", ""),
                "messages": []
            }
            
            '''
            user_info = comment_item.get("user_info", {})
            comment_id = comment_item.get("id")
            comment_pictures = [item.get("url_default", "") for item in comment_item.get("pictures", [])]
            target_comment = comment_item.get("target_comment", {})
            local_db_item = {
                "comment_id": comment_id, # 评论id
                "create_time": comment_item.get("create_time"), # 评论时间
                "ip_location": comment_item.get("ip_location"), # ip地址
                "note_id": note_id, # 帖子id
                "content": comment_item.get("content"), # 评论内容
                "user_id": user_info.get("user_id"), # 用户id
                "nickname": user_info.get("nickname"), # 用户昵称
                "avatar": user_info.get("image"), # 用户头像
                "sub_comment_count": comment_item.get("sub_comment_count", 0), # 子评论数
                "pictures": ",".join(comment_pictures), # 评论图片
                "parent_comment_id": target_comment.get("id", 0), # 父评论id
                "last_modify_ts": utils.get_current_timestamp(), # 最后更新时间戳（MediaCrawler程序生成的，主要用途在db存储的时候记录一条记录最新更新时间）
                "like_count": comment_item.get("like_count", 0),
            }
            '''

            comment_pictures = [item.get("url_default", "") for item in comment.get("pictures", [])]
            # 添加根评论
            conversation["messages"].append({
                "comment_id": comment.get["id"],
                "user_id": comment.get("user_info", {}).get("user_id"),
                "user_name": comment.get("user_info", {}).get("nickname"),
                "content": comment.get("content"),
                "pictures": ",".join(comment_pictures), # 评论图片
                "create_time": comment.get("create_time"),
                "is_author": comment.get("is_author", False),
                "like_count": comment.get("like_count", 0)
            })
            
            # 添加子评论
            for sub_comment in comment.get("sub_comments", []):
                sub_comment_pictures = [item.get("url_default", "") for item in sub_comment.get("pictures", [])]
                conversation["messages"].append({
                    "comment_id": sub_comment["id"],
                    "parent_id": comment["id"],
                    "user_id": sub_comment.get("user_info", {}).get("user_id"),
                    "user_name": sub_comment.get("user_info", {}).get("nickname"),
                    "content": sub_comment.get("content"),
                    "pictures": ",".join(sub_comment_pictures), # 评论图片'
                    "create_time": sub_comment.get("create_time"),
                    "is_author": sub_comment.get("is_author", False),
                    "like_count": sub_comment.get("like_count", 0)
                })
            
            # 只保留有对话的评论（至少有一个回复）
            if len(conversation["messages"]) > 0:
                conversations.append(conversation)
        
        return conversations

    async def save_note_detail_to_jsonl(self, note_id: str, note_detail: Dict):
        """保存笔记详情到JSONL文件，供后续处理
        
        Args:
            note_id: 笔记ID
            note_detail: 笔记详情
        """
        # 创建临时目录
        temp_dir = os.path.join(config.COMMENT_CORPUS_DIR, "temp")
        os.makedirs(temp_dir, exist_ok=True)
        
        # 保存笔记详情
        output_file = os.path.join(temp_dir, f"{note_id}_detail.json")
        async with aiofiles.open(output_file, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(note_detail, ensure_ascii=False))
        
        utils.logger.info(f"[XiaoHongShuCrawler.save_note_detail_to_jsonl] Saved note detail for {note_id}")

    async def get_comments(self, note_id: str, xsec_token: str, note_detail: Dict = None, semaphore: asyncio.Semaphore = None):
        """Get note comments with keyword filtering and quantity limitation"""
        if semaphore:
            async with semaphore:
                return await self._get_comments_impl(note_id, xsec_token, note_detail)
        else:
            return await self._get_comments_impl(note_id, xsec_token, note_detail)

    async def _get_comments_impl(self, note_id: str, xsec_token: str, note_detail: Dict = None):
        """评论获取实现"""
        utils.logger.info(
            f"[XiaoHongShuCrawler.get_comments] Begin get note id comments {note_id}"
        )
        # When proxy is not enabled, increase the crawling interval
        if config.ENABLE_IP_PROXY:
            crawl_interval = random.random()
        else:
            crawl_interval = random.uniform(1, config.CRAWLER_MAX_SLEEP_SEC)
        
        # 获取评论
        comments = await self.xhs_client.get_note_all_comments(
            note_id=note_id,
            xsec_token=xsec_token,
            crawl_interval=crawl_interval,
            callback=xhs_store.batch_update_xhs_note_comments,
            max_count=CRAWLER_MAX_COMMENTS_COUNT_SINGLENOTES,
        )

        # 如果启用了评论对话保留，则保存评论到JSONL
        if config.ENABLE_COMMENT_CONVERSATION and config.SAVE_DATA_OPTION == "json":
            # 目前只支持json格式的转化（其他格式CSV、DB会报错）
            await self.store.convert_comments_to_conversations()

    async def stop(self):
        """Stop crawler and clean up resources"""
        utils.logger.info("[XiaoHongShuCrawler.stop] Begin stop xiaohongshu crawler ...")
        # 安全关闭浏览器
  
        try:
            if hasattr(self, 'playwright') and self.playwright:
                await self.playwright.stop()
        except Exception as e:
            utils.logger.error(f"[XiaoHongShuCrawler.stop] Error stopping playwright: {e}")
        
        utils.logger.info("[XiaoHongShuCrawler.stop] Stop xiaohongshu crawler successful")
