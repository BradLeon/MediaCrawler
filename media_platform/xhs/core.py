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
from proxy.providers.kuaidl_tunnel_proxy import KuaiDaiLiTunnelProxy
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
        try:
            playwright_proxy, httpx_proxy = None, None
            if config.ENABLE_IP_PROXY:
                #ip_proxy_pool = await create_ip_pool(
                #    config.IP_PROXY_POOL_COUNT, enable_validate_ip=True
                #)
                kdl_tunnel_proxy = KuaiDaiLiTunnelProxy()
                # ip_proxy_info: IpInfoModel = await ip_proxy_pool.get_proxy()
                playwright_proxy, httpx_proxy = self.format_proxy_info(
                    kdl_tunnel_proxy
                )

            async with async_playwright() as playwright:
                # 先启动浏览器但不使用持久化上下文
                chromium = playwright.chromium
                self.browser_context = await self.launch_browser(
                    chromium, None, self.user_agent, headless=config.HEADLESS
                )
                
                # 注入stealth.min.js
                await self.browser_context.add_init_script(path="libs/stealth.min.js")
                # add webId cookie to avoid sliding captcha
                await self.browser_context.add_cookies(
                    [
                        {
                            "name": "webId",
                            "value": "xxx123",
                            "domain": ".xiaohongshu.com",
                            "path": "/",
                        }
                    ]
                )
                self.context_page = await self.browser_context.new_page()
                await self.context_page.goto(self.index_url)

                # 创建客户端和尝试登录
                self.xhs_client = await self.create_xhs_client(httpx_proxy)
                login_successful = await self.xhs_client.pong()
                
                # 如果启用了保存登录状态但当前没有登录成功，尝试从保存的cookies登录
                if not login_successful and config.SAVE_LOGIN_STATE:
                    login_obj = XiaoHongShuLogin(
                        login_type="cookie",
                        login_phone="",
                        browser_context=self.browser_context,
                        context_page=self.context_page,
                        cookie_str=config.COOKIES,
                    )
                    # 尝试加载保存的cookies
                    cookies = await login_obj.load_saved_cookies()
                    if cookies:
                        await self.browser_context.add_cookies(cookies)
                        await self.context_page.reload()
                        # 更新客户端cookies
                        await self.xhs_client.update_cookies(browser_context=self.browser_context)
                        login_successful = await self.xhs_client.pong()
                
                # 如果仍未登录成功，使用配置的登录方式
                if not login_successful:
                    login_obj = XiaoHongShuLogin(
                        login_type=config.LOGIN_TYPE,
                        login_phone="",
                        browser_context=self.browser_context,
                        context_page=self.context_page,
                        cookie_str=config.COOKIES,
                    )
                    await login_obj.begin()
                    await self.xhs_client.update_cookies(browser_context=self.browser_context)
                    
                    # 如果启用了保存登录状态，保存cookies
                    if config.SAVE_LOGIN_STATE:
                        await login_obj.save_cookies()

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
        except Exception as e:
            utils.logger.error(f"爬虫运行过程中发生错误: {e}")
            # 可以选择是否继续执行或退出
            # 如果是严重错误，可以在这里调用self.stop()
        finally:
            # 确保资源被正确释放
            await self.stop()

    async def search(self) -> None:
        """Search for notes and retrieve their comment information."""
        utils.logger.info(
            "[XiaoHongShuCrawler.search] Begin search xiaohongshu keywords"
        )
        # 获取当前登录用户信息
        current_user_account = await self.xhs_client.get_current_user_nickname()
        
        xhs_limit_count = 20  # xhs limit page fixed value
        if config.CRAWLER_MAX_NOTES_COUNT < xhs_limit_count:
            config.CRAWLER_MAX_NOTES_COUNT = xhs_limit_count
        start_page = config.START_PAGE
        for keyword in config.KEYWORDS.split(","):
            search_result_item = {}
            search_result_list = []

            source_keyword_var.set(keyword)
            utils.logger.info(
                f"[XiaoHongShuCrawler.search] Current search keyword: {keyword}"
            )
            page = 1
            rank = 1
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
                    '''
                    utils.logger.info(
                        f"[XiaoHongShuCrawler.search] Search notes res:{notes_res}"
                    )
                    
                    utils.logger.info(
                        f"[XiaoHongShuCrawler.search] Search size notes res:{len(notes_res.get('items', {}))}"
                    )
                    '''
                    if not notes_res or not notes_res.get("has_more", False):
                        utils.logger.info("No more content!")
                        break
                    
                   
                    # todo: 获取排序结果，并记录
                    for index, post_item in enumerate(notes_res.get("items", {})):
                        search_result_item = {
                            "keyword": keyword,
                            "search_account": current_user_account,  # 使用当前用户昵称
                            "rank": rank,
                            "note_id": post_item.get("id"),
                        }
                        search_result_list.append(search_result_item)
                        rank += 1

                    
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
                    # utils.logger.info(f"[XiaoHongShuCrawler.search] Note details: {note_details}")
                    
                    page += 1
                    
                    #await self.batch_get_note_comments(note_ids, xsec_tokens)
                except DataFetchError:
                    utils.logger.error(
                        "[XiaoHongShuCrawler.search] Get note detail error"
                    )
                    break
                # todo: 每个keyword搜索结果保存一次
            utils.logger.info(
                            f"[XiaoHongShuCrawler.search] search_result_list: {search_result_list}"
                            )
            await self.store.store_search_result(search_result_list) 
        

    async def get_creators_and_notes(self) -> None:
        """Get creator's notes and retrieve their comment information."""
        utils.logger.info(
            "[XiaoHongShuCrawler.get_creators_and_notes] Begin get xiaohongshu creators"
        )
        for user_id in config.XHS_CREATOR_ID_LIST:
            # 在获取创作者信息前模拟人类行为
            # await self.simulate_human_behavior(self.context_page)
            
            # get creator detail info from web html content
            createor_info: Dict = await self.xhs_client.get_creator_info(
                user_id=user_id
            )
            
            # 在获取创作者信息后模拟人类行为
            await self.simulate_human_behavior(self.context_page)
            
            if createor_info:
                await xhs_store.save_creator(user_id, creator=createor_info)

            # When proxy is not enabled, increase the crawling interval
            if config.ENABLE_IP_PROXY:
                crawl_interval = random.uniform(1, config.CRAWLER_MAX_SLEEP_SEC)
            else:
                crawl_interval = random.uniform(1, config.CRAWLER_MAX_SLEEP_SEC)
            # Get all note information of the creator
            all_notes_list = await self.xhs_client.get_all_notes_by_creator(
                user_id=user_id,
                crawl_interval=crawl_interval,
                max_count=config.CRAWLER_MAX_NOTES_COUNT,
                callback=self.fetch_creator_notes_detail
            )
            
            print("[XiaoHongShuCrawler.get_all_notes_by_creator] after crawler get all notes size:", len(all_notes_list))


    async def fetch_creator_notes_detail(self, note_list: List[Dict]) -> List[str]:
        """
        Concurrently obtain the specified post list and save the data
        """
        filtered_note_list = list()
        xsec_tokens = list()
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        task_list = [
            self.get_note_detail_async_task(
                note_id=post_item.get("note_id"),
                xsec_source="pc_search",
                xsec_token=post_item.get("xsec_token"),
                semaphore=semaphore,
            )
            for post_item in note_list
        ]

        note_details = await asyncio.gather(*task_list)
        for note_detail in note_details:
            if note_detail:
                try:
                    # 增加过滤条件：时间过滤条件&点赞数过滤条件，一般认为评论数大于点赞数，可能是活动或水军导致，分析意义不大
                    last_update_time = note_detail.get("last_update_time", 0) 
                    interact_info = note_detail.get("interact_info", {})
                    
                    # 使用安全转换函数
                    comment_count = self.safe_int_convert(interact_info.get("comment_count", 0))
                    liked_count = self.safe_int_convert(interact_info.get("liked_count", 0))
                    
                    if (comment_count < config.COMMENT_COUNT_THRESHOLD or 
                        liked_count < comment_count or 
                        last_update_time < config.LAST_UPDATE_TIME_THRESHOLD):
                        continue
                        
                    await xhs_store.update_xhs_note(note_detail)
                    filtered_note_list.append(note_detail.get("note_id"))
                    xsec_tokens.append(note_detail.get("xsec_token"))
                except Exception as e:
                    # 捕获处理单个笔记时的所有异常，防止一个笔记的问题影响整个流程
                    utils.logger.error(f"处理笔记详情时出错: {e}, note_id: {note_detail.get('note_id', 'unknown')}")
                    continue
        
        # 获取过滤后的笔记的评论
        await self.batch_get_note_comments(filtered_note_list, xsec_tokens)
    
        # test
        print("[XiaoHongShuCrawler.fetch_creator_notes_detail ] after filtered note_list size:", len(filtered_note_list))
        return filtered_note_list

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
                try:
                    # 增加过滤条件：时间过滤条件&点赞数过滤条件，一般认为评论数大于点赞数，可能是活动或水军导致，分析意义不大
                    last_update_time = note_detail.get("last_update_time", 0) 
                    interact_info = note_detail.get("interact_info", {})
                    
                    # 使用安全转换函数
                    comment_count = self.safe_int_convert(interact_info.get("comment_count", 0))
                    liked_count = self.safe_int_convert(interact_info.get("liked_count", 0))
                    
                    if (comment_count < config.COMMENT_COUNT_THRESHOLD or 
                        liked_count < comment_count or 
                        last_update_time < config.LAST_UPDATE_TIME_THRESHOLD):
                        continue

                    need_get_comment_note_ids.append(note_detail.get("note_id", ""))
                    xsec_tokens.append(note_detail.get("xsec_token", ""))
                    await xhs_store.update_xhs_note(note_detail)
                    await self.get_notice_media(note_detail)
                except Exception as e:
                    utils.logger.error(f"处理指定笔记时出错: {e}, note_id: {note_detail.get('note_id', 'unknown')}")
                    continue
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
                crawl_interval = random.uniform(1, config.CRAWLER_MAX_SLEEP_SEC)
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
                '''
                if not note_detail_from_html:
                    # 如果网页版笔记详情获取失败，则尝试不使用cookie获取
                    utils.logger.info(
                        f"[XiaoHongShuCrawler.get_note_detail_async_task] First attempt failed, trying without cookie for note_id: {note_id}"
                    )
                    note_detail_from_html = (
                        await self.xhs_client.get_note_by_id_from_html(
                            note_id, xsec_source, xsec_token, enable_cookie=False
                        )
                    )
                '''
                if not note_detail_from_html:
                    # 如果网页版笔记详情获取失败，则尝试API获取
                    utils.logger.info(
                        f"[XiaoHongShuCrawler.get_note_detail_async_task] HTML parsing failed, trying API for note_id: {note_id}"
                    )
                    note_detail_from_api: Optional[Dict] = (
                        await self.xhs_client.get_note_by_id(
                            note_id, xsec_source, xsec_token
                        )
                    )
                    if note_detail_from_api:
                        utils.logger.info(
                            f"[XiaoHongShuCrawler.get_note_detail_async_task] Successfully got note detail from API for note_id: {note_id}, liked count: {note_detail_from_api.get('interact_info', {}).get('liked_count')}"
                        )
                    else:
                        utils.logger.warning(
                            f"[XiaoHongShuCrawler.get_note_detail_async_task] Failed to get note detail from both HTML and API for note_id: {note_id}"
                        )
                note_detail = note_detail_from_html or note_detail_from_api
                if note_detail:
                    note_detail.update(
                        {"xsec_token": xsec_token, "xsec_source": xsec_source}
                    )
                    utils.logger.info(
                        f"[XiaoHongShuCrawler.get_note_detail_async_task] Successfully processed note_id: {note_id}"
                    )
                    return note_detail
                else:
                    utils.logger.error(
                        f"[XiaoHongShuCrawler.get_note_detail_async_task] All methods failed for note_id: {note_id}"
                    )
                    return None
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
            
            # 每处理3-5个请求后随机模拟人类行为
            if index % random.randint(3, 5) == 0:
                await self.simulate_human_behavior(self.context_page)
        
        await asyncio.gather(*task_list)

    async def get_comments(self, note_id: str, xsec_token: str, semaphore: asyncio.Semaphore = None):
        """Get note comments with keyword filtering and quantity limitation"""
        async with semaphore:
            utils.logger.info(
                f"[XiaoHongShuCrawler.get_comments] Begin get note id comments {note_id}"
            )
            # When proxy is not enabled, increase the crawling interval
            if config.ENABLE_IP_PROXY:
                crawl_interval = random.uniform(1, config.CRAWLER_MAX_SLEEP_SEC)
            else:
                crawl_interval = random.uniform(1, config.CRAWLER_MAX_SLEEP_SEC)
            
            await self.xhs_client.get_note_all_comments(
                note_id=note_id,
                xsec_token=xsec_token,
                crawl_interval=crawl_interval,
                callback=xhs_store.batch_update_xhs_note_comments,
                max_count=config.CRAWLER_MAX_COMMENTS_COUNT_SINGLENOTES,
            )

        # 如果启用了评论对话保留，则保存评论到JSONL
        # if config.ENABLE_COMMENT_CONVERSATION and config.SAVE_DATA_OPTION == "json":
            # 目前只支持json格式的转化（其他格式CSV、DB会报错）
            # await self.store.convert_comments_to_conversations()

    @staticmethod
    def format_proxy_info(
        kdl_tunnel_proxy: KuaiDaiLiTunnelProxy,
    ) -> Tuple[Optional[Dict], Optional[Dict]]:
        """format proxy info for playwright and httpx"""
        '''
        playwright_proxy = {
            "server": f"{ip_proxy_info.protocol}{ip_proxy_info.ip}:{ip_proxy_info.port}",
            "username": ip_proxy_info.user,
            "password": ip_proxy_info.password,
        }

        httpx_proxy = {
            f"{ip_proxy_info.protocol}": f"http://{ip_proxy_info.user}:{ip_proxy_info.password}@{ip_proxy_info.ip}:{ip_proxy_info.port}"
        }
        '''
            
        playwright_proxy = {
            "server": kdl_tunnel_proxy.tunnel,
            "username": kdl_tunnel_proxy.user,
            "password": kdl_tunnel_proxy.password,
        }

        httpx_proxy = {
            "http://": f"http://{kdl_tunnel_proxy.user}:{kdl_tunnel_proxy.password}@{kdl_tunnel_proxy.tunnel}",
            "https://": f"http://{kdl_tunnel_proxy.user}:{kdl_tunnel_proxy.password}@{kdl_tunnel_proxy.tunnel}"
        }

        # test
        print("playwright_proxy:", playwright_proxy)
        print("httpx_proxy:", httpx_proxy)
        return playwright_proxy, httpx_proxy

    async def create_xhs_client(self, httpx_proxy: Optional[str]) -> XiaoHongShuClient:
        """Create xhs client"""
        utils.logger.info(
            "[XiaoHongShuCrawler.create_xhs_client] Begin create xiaohongshu API client ..."
        )
        cookie_str, cookie_dict = utils.convert_cookies(
            await self.browser_context.cookies()
        )
        
        # 添加更多的请求头，使请求更接近真实浏览器
        headers = {
            "User-Agent": self.user_agent,
            "Cookie": cookie_str,
            "Origin": "https://www.xiaohongshu.com",
            "Referer": "https://www.xiaohongshu.com",
            "Content-Type": "application/json;charset=UTF-8",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "sec-ch-ua": '"Google Chrome";v="133", "Not(A:Brand";v="8", "Chromium";v="133"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }
        
        xhs_client_obj = XiaoHongShuClient(
            proxies=httpx_proxy,
            headers=headers,
            playwright_page=self.context_page,
            cookie_dict=cookie_dict,
        )
        return xhs_client_obj

    async def launch_browser(
        self,
        chromium: BrowserType,
        playwright_proxy: Optional[Dict],
        user_agent: Optional[str],
        headless: bool = True
    ) -> BrowserContext:
        """Launch browser and create browser context"""
        utils.logger.info("[XiaoHongShuCrawler.launch_browser] Begin launch chromium browser ...")
        
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

    async def rotate_fingerprint(self):
        """定期更换浏览器指纹"""
        # 关闭旧上下文
        await self.browser_context.close()
        
        # 随机选择一个新的用户代理
        new_user_agent = random.choice([
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
        ])
        
        # 创建新的浏览器上下文
        self.browser_context = await self.launch_browser(
            self.playwright.chromium,
            self.playwright_proxy,
            new_user_agent,
            headless=config.HEADLESS
        )
        
        # 重新注入stealth脚本
        await self.browser_context.add_init_script(path="libs/stealth.min.js")
        
        # 创建新页面
        self.context_page = await self.browser_context.new_page()
        await self.context_page.goto(self.index_url)
        
        # 更新客户端
        await self.xhs_client.update_cookies(browser_context=self.browser_context)

    async def human_like_click(self, element):
        """模拟人类点击行为"""
        box = await element.bounding_box()
        x = box["x"] + box["width"] * random.uniform(0.3, 0.7)
        y = box["y"] + box["height"] * random.uniform(0.3, 0.7)
        
        await self.context_page.mouse.move(x, y)
        await asyncio.sleep(random.uniform(0.1, 0.3))
        await self.context_page.mouse.down()
        await asyncio.sleep(random.uniform(0.05, 0.15))
        await self.context_page.mouse.up()

    async def simulate_human_behavior(self, page):
        """模拟人类浏览行为"""
        # 随机滚动
        await page.evaluate("""
        () => {
            const scrollHeight = Math.floor(Math.random() * 100);
            window.scrollBy(0, scrollHeight);
        }
        """)
        
        # 随机移动鼠标
        await page.mouse.move(
            x=random.randint(100, 500),
            y=random.randint(100, 500),
            steps=random.randint(5, 10)
        )
        
        # 随机暂停
        await asyncio.sleep(random.uniform(0.5, 2.0))

    def safe_int_convert(self, value, default=0):
        """安全地将值转换为整数
        
        处理以下情况:
        - 数字字符串: "123" -> 123
        - 带加号的数字: "10+" -> 10
        - 带千分位的数字: "1,234" -> 1234
        - 带单位的数字: "1.2k" -> 1200, "1.2w" -> 12000
        - 非数字: 返回默认值
        """
        if value is None:
            return default
        
        if isinstance(value, int):
            return value
        
        if isinstance(value, float):
            return int(value)
        
        if not isinstance(value, str):
            return default
        
        # 处理空字符串
        if not value.strip():
            return default
        
        try:
            # 处理带加号的数字 (例如 "10+")
            if "+" in value:
                value = value.replace("+", "")
            
            # 处理带千分位的数字 (例如 "1,234")
            value = value.replace(",", "")
            
            # 处理带单位的数字
            if value.lower().endswith('k'):
                # 例如 "1.2k" -> 1200
                return int(float(value[:-1]) * 1000)
            elif value.lower().endswith('w'):
                # 例如 "1.2w" -> 12000 (万)
                return int(float(value[:-1]) * 10000)
            elif value.lower().endswith('m'):
                # 例如 "1.2m" -> 1200000
                return int(float(value[:-1]) * 1000000)
            
            # 尝试直接转换为整数
            return int(float(value))
        except (ValueError, TypeError):
            utils.logger.warning(f"无法将值 '{value}' 转换为整数，使用默认值 {default}")
            return default

    def validate_note_detail(self, note_detail):
        """验证笔记详情数据的完整性"""
        required_fields = ["note_id", "user", "type"]
        for field in required_fields:
            if field not in note_detail:
                utils.logger.warning(f"笔记缺少必要字段: {field}, note_id: {note_detail.get('note_id', 'unknown')}")
                return False
        return True
