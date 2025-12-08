# -*- coding: utf-8 -*-
# Copyright (c) 2025 relakkes@gmail.com
#
# This file is part of MediaCrawler project.
# Repository: https://github.com/NanmiCoder/MediaCrawler
# Licensed under NON-COMMERCIAL LEARNING LICENSE 1.1
#
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

from playwright.async_api import (
    BrowserContext,
    BrowserType,
    Page,
    Playwright,
    async_playwright,
)
from tenacity import RetryError

import config
from base.base_crawler import AbstractCrawler
from config import CRAWLER_MAX_COMMENTS_COUNT_SINGLENOTES
from model.m_xiaohongshu import NoteUrlInfo, CreatorUrlInfo
from proxy.proxy_ip_pool import IpInfoModel, create_ip_pool
from proxy.providers.kuaidl_tunnel_proxy import KuaiDaiLiTunnelProxy
from store import xhs as xhs_store
from tools import utils
from tools.cdp_browser import CDPBrowserManager
from var import crawler_type_var, source_keyword_var

from .client import XiaoHongShuClient
from .exception import DataFetchError
from .field import SearchSortType
from .help import parse_note_info_from_note_url, parse_creator_info_from_url, get_search_id
from .login import XiaoHongShuLogin


class XiaoHongShuCrawler(AbstractCrawler):
    context_page: Page
    xhs_client: XiaoHongShuClient
    browser_context: BrowserContext
    cdp_manager: Optional[CDPBrowserManager]

    def __init__(self) -> None:
        self.index_url = "https://www.xiaohongshu.com"
        self.user_agent = config.UA if hasattr(config, 'UA') and config.UA else "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
        self.cdp_manager = None
        self.ip_proxy_pool = None
        # 创建并保存 store 实例 (Fork自定义)
        from store.xhs import XhsStoreFactory
        self.store = XhsStoreFactory.create_store()

    async def start(self) -> None:
        try:
            playwright_proxy_format, httpx_proxy_format = None, None
            if config.ENABLE_IP_PROXY:
                # Fork自定义: 支持KDL隧道代理
                if hasattr(config, 'IP_PROXY_PROVIDER_NAME') and config.IP_PROXY_PROVIDER_NAME == "kuaidaili_tunnel":
                    kdl_tunnel_proxy = KuaiDaiLiTunnelProxy()
                    playwright_proxy_format, httpx_proxy_format = self.format_proxy_info(kdl_tunnel_proxy)
                else:
                    self.ip_proxy_pool = await create_ip_pool(config.IP_PROXY_POOL_COUNT, enable_validate_ip=True)
                    ip_proxy_info: IpInfoModel = await self.ip_proxy_pool.get_proxy()
                    playwright_proxy_format, httpx_proxy_format = utils.format_proxy_info(ip_proxy_info)

            async with async_playwright() as playwright:
                # 根据配置选择启动模式
                if config.ENABLE_CDP_MODE:
                    utils.logger.info("[XiaoHongShuCrawler] 使用CDP模式启动浏览器")
                    self.browser_context = await self.launch_browser_with_cdp(
                        playwright,
                        playwright_proxy_format,
                        self.user_agent,
                        headless=config.CDP_HEADLESS,
                    )
                else:
                    utils.logger.info("[XiaoHongShuCrawler] 使用标准模式启动浏览器")
                    chromium = playwright.chromium
                    self.browser_context = await self.launch_browser(
                        chromium,
                        playwright_proxy_format,
                        self.user_agent,
                        headless=config.HEADLESS,
                    )
                    # stealth.min.js is a js script to prevent the website from detecting the crawler.
                    await self.browser_context.add_init_script(path="libs/stealth.min.js")
                    # Fork自定义: 添加webId cookie避免滑动验证码
                    await self.browser_context.add_cookies([
                        {
                            "name": "webId",
                            "value": "xxx123",
                            "domain": ".xiaohongshu.com",
                            "path": "/",
                        }
                    ])

                self.context_page = await self.browser_context.new_page()
                await self.context_page.goto(self.index_url)

                # Create a client to interact with the xiaohongshu website.
                self.xhs_client = await self.create_xhs_client(httpx_proxy_format)
                login_successful = await self.xhs_client.pong()

                # Fork自定义: 尝试从保存的cookies登录
                if not login_successful and config.SAVE_LOGIN_STATE:
                    login_obj = XiaoHongShuLogin(
                        login_type="cookie",
                        login_phone="",
                        browser_context=self.browser_context,
                        context_page=self.context_page,
                        cookie_str=config.COOKIES,
                    )
                    cookies = await login_obj.load_saved_cookies()
                    if cookies:
                        await self.browser_context.add_cookies(cookies)
                        await self.context_page.reload()
                        await self.xhs_client.update_cookies(browser_context=self.browser_context)
                        login_successful = await self.xhs_client.pong()

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

                    # Fork自定义: 保存cookies
                    if config.SAVE_LOGIN_STATE:
                        await login_obj.save_cookies()

                crawler_type_var.set(config.CRAWLER_TYPE)
                if config.CRAWLER_TYPE == "search":
                    await self.search()
                elif config.CRAWLER_TYPE == "detail":
                    await self.get_specified_notes()
                elif config.CRAWLER_TYPE == "creator":
                    await self.get_creators_and_notes()
                else:
                    pass

                utils.logger.info("[XiaoHongShuCrawler.start] Xhs Crawler finished ...")
        except Exception as e:
            utils.logger.error(f"爬虫运行过程中发生错误: {e}")
        finally:
            await self.stop()

    async def search(self) -> None:
        """Search for notes and retrieve their comment information."""
        utils.logger.info("[XiaoHongShuCrawler.search] Begin search xiaohongshu keywords")

        # Fork自定义: 获取当前登录用户信息用于搜索结果记录
        current_user_account = await self.xhs_client.get_current_user_nickname()

        xhs_limit_count = 20
        if config.CRAWLER_MAX_NOTES_COUNT < xhs_limit_count:
            config.CRAWLER_MAX_NOTES_COUNT = xhs_limit_count
        start_page = config.START_PAGE

        for keyword in config.KEYWORDS.split(","):
            # Fork自定义: 记录搜索结果排名
            search_result_list = []

            source_keyword_var.set(keyword)
            utils.logger.info(f"[XiaoHongShuCrawler.search] Current search keyword: {keyword}")
            page = 1
            rank = 1
            search_id = get_search_id()

            while (page - start_page + 1) * xhs_limit_count <= config.CRAWLER_MAX_NOTES_COUNT:
                if page < start_page:
                    utils.logger.info(f"[XiaoHongShuCrawler.search] Skip page {page}")
                    page += 1
                    continue

                try:
                    utils.logger.info(f"[XiaoHongShuCrawler.search] search xhs keyword: {keyword}, page: {page}")
                    note_ids: List[str] = []
                    xsec_tokens: List[str] = []
                    notes_res = await self.xhs_client.get_note_by_keyword(
                        keyword=keyword,
                        search_id=search_id,
                        page=page,
                        sort=(SearchSortType(config.SORT_TYPE) if config.SORT_TYPE != "" else SearchSortType.GENERAL),
                    )
                    utils.logger.info(f"[XiaoHongShuCrawler.search] Search notes res:{notes_res}")
                    if not notes_res or not notes_res.get("has_more", False):
                        utils.logger.info("No more content!")
                        break

                    # Fork自定义: 记录搜索排名
                    for post_item in notes_res.get("items", {}):
                        if post_item.get("model_type") not in ("rec_query", "hot_query"):
                            search_result_list.append({
                                "keyword": keyword,
                                "search_account": current_user_account,
                                "rank": rank,
                                "note_id": post_item.get("id"),
                            })
                            rank += 1

                    semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
                    task_list = [
                        self.get_note_detail_async_task(
                            note_id=post_item.get("id"),
                            xsec_source=post_item.get("xsec_source"),
                            xsec_token=post_item.get("xsec_token"),
                            semaphore=semaphore,
                        ) for post_item in notes_res.get("items", {}) if post_item.get("model_type") not in ("rec_query", "hot_query")
                    ]
                    note_details = await asyncio.gather(*task_list)
                    for note_detail in note_details:
                        if note_detail:
                            await xhs_store.update_xhs_note(note_detail)
                            await self.get_notice_media(note_detail)
                            note_ids.append(note_detail.get("note_id"))
                            xsec_tokens.append(note_detail.get("xsec_token"))
                    page += 1
                    utils.logger.info(f"[XiaoHongShuCrawler.search] Note details: {note_details}")
                    await self.batch_get_note_comments(note_ids, xsec_tokens)

                    await asyncio.sleep(config.CRAWLER_MAX_SLEEP_SEC)
                    utils.logger.info(f"[XiaoHongShuCrawler.search] Sleeping for {config.CRAWLER_MAX_SLEEP_SEC} seconds after page {page-1}")
                except DataFetchError:
                    utils.logger.error("[XiaoHongShuCrawler.search] Get note detail error")
                    break

            # Fork自定义: 保存搜索结果排名
            if search_result_list:
                utils.logger.info(f"[XiaoHongShuCrawler.search] search_result_list: {search_result_list}")
                await self.store.store_search_result(search_result_list)

    async def get_creators_and_notes(self) -> None:
        """Get creator's notes and retrieve their comment information."""
        utils.logger.info("[XiaoHongShuCrawler.get_creators_and_notes] Begin get xiaohongshu creators")
        for creator_url in config.XHS_CREATOR_ID_LIST:
            try:
                # 尝试解析URL格式的创作者信息
                if creator_url.startswith("http"):
                    creator_info: CreatorUrlInfo = parse_creator_info_from_url(creator_url)
                    utils.logger.info(f"[XiaoHongShuCrawler.get_creators_and_notes] Parse creator URL info: {creator_info}")
                    user_id = creator_info.user_id
                    xsec_token = creator_info.xsec_token
                    xsec_source = creator_info.xsec_source
                else:
                    # 兼容旧的纯ID格式
                    user_id = creator_url
                    xsec_token = None
                    xsec_source = None

                # Fork自定义: 模拟人类行为
                await self.simulate_human_behavior(self.context_page)

                # get creator detail info from web html content
                createor_info: Dict = await self.xhs_client.get_creator_info(
                    user_id=user_id,
                    xsec_token=xsec_token,
                    xsec_source=xsec_source
                )
                if createor_info:
                    await xhs_store.save_creator(user_id, creator=createor_info)
            except ValueError as e:
                utils.logger.error(f"[XiaoHongShuCrawler.get_creators_and_notes] Failed to parse creator URL: {e}")
                continue

            # Use fixed crawling interval
            crawl_interval = config.CRAWLER_MAX_SLEEP_SEC
            # Get all note information of the creator
            all_notes_list = await self.xhs_client.get_all_notes_by_creator(
                user_id=user_id,
                crawl_interval=crawl_interval,
                callback=self.fetch_creator_notes_detail,
                xsec_token=xsec_token if 'xsec_token' in dir() else None,
                xsec_source=xsec_source if 'xsec_source' in dir() else None,
            )

            note_ids = []
            xsec_tokens = []
            for note_item in all_notes_list:
                note_ids.append(note_item.get("note_id"))
                xsec_tokens.append(note_item.get("xsec_token"))
            await self.batch_get_note_comments(note_ids, xsec_tokens)

    async def fetch_creator_notes_detail(self, note_list: List[Dict]) -> List[str]:
        """
        Concurrently obtain the specified post list and save the data
        Fork自定义: 添加过滤逻辑
        """
        filtered_note_list = []
        xsec_tokens = []
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        task_list = [
            self.get_note_detail_async_task(
                note_id=post_item.get("note_id"),
                xsec_source=post_item.get("xsec_source", "pc_search"),
                xsec_token=post_item.get("xsec_token"),
                semaphore=semaphore,
            ) for post_item in note_list
        ]

        note_details = await asyncio.gather(*task_list)
        for note_detail in note_details:
            if note_detail:
                try:
                    # Fork自定义: 过滤条件
                    last_update_time = note_detail.get("last_update_time", 0)
                    interact_info = note_detail.get("interact_info", {})

                    comment_count = self.safe_int_convert(interact_info.get("comment_count", 0))
                    liked_count = self.safe_int_convert(interact_info.get("liked_count", 0))

                    # 过滤条件：评论数阈值、点赞数、更新时间
                    if (comment_count < config.COMMENT_COUNT_THRESHOLD or
                        liked_count < comment_count or
                        last_update_time < config.LAST_UPDATE_TIME_THRESHOLD):
                        continue

                    await xhs_store.update_xhs_note(note_detail)
                    await self.get_notice_media(note_detail)
                    filtered_note_list.append(note_detail.get("note_id"))
                    xsec_tokens.append(note_detail.get("xsec_token"))
                except Exception as e:
                    utils.logger.error(f"处理笔记详情时出错: {e}, note_id: {note_detail.get('note_id', 'unknown')}")
                    continue

        await self.batch_get_note_comments(filtered_note_list, xsec_tokens)
        utils.logger.info(f"[XiaoHongShuCrawler.fetch_creator_notes_detail] after filtered note_list size: {len(filtered_note_list)}")
        return filtered_note_list

    async def get_specified_notes(self):
        """
        Get the information and comments of the specified post
        must be specified note_id, xsec_source, xsec_token
        """
        get_note_detail_task_list = []
        for full_note_url in config.XHS_SPECIFIED_NOTE_URL_LIST:
            note_url_info: NoteUrlInfo = parse_note_info_from_note_url(full_note_url)
            utils.logger.info(f"[XiaoHongShuCrawler.get_specified_notes] Parse note url info: {note_url_info}")
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
                    # Fork自定义: 过滤条件
                    last_update_time = note_detail.get("last_update_time", 0)
                    interact_info = note_detail.get("interact_info", {})

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
        Fork自定义: 多方法获取笔记详情
        """
        note_detail = None
        utils.logger.info(f"[get_note_detail_async_task] Begin get note detail, note_id: {note_id}")
        async with semaphore:
            crawl_interval = random.uniform(1, config.CRAWLER_MAX_SLEEP_SEC)
            try:
                # Fork自定义: 多方法获取笔记详情
                # 方法1: 尝试HTML解析(带cookie)
                note_detail = await self.xhs_client.get_note_by_id_from_html(
                    note_id, xsec_source, xsec_token, enable_cookie=True
                )
                time.sleep(crawl_interval)

                # 方法2: 如果HTML失败，尝试API
                if not note_detail:
                    utils.logger.info(f"[get_note_detail_async_task] HTML parsing failed, trying API for note_id: {note_id}")
                    try:
                        note_detail = await self.xhs_client.get_note_by_id(note_id, xsec_source, xsec_token)
                    except RetryError:
                        pass

                if not note_detail:
                    utils.logger.warning(f"[get_note_detail_async_task] All methods failed for note_id: {note_id}")
                    return None

                note_detail.update({"xsec_token": xsec_token, "xsec_source": xsec_source})

                await asyncio.sleep(config.CRAWLER_MAX_SLEEP_SEC)
                utils.logger.info(f"[get_note_detail_async_task] Sleeping for {config.CRAWLER_MAX_SLEEP_SEC} seconds after fetching note {note_id}")

                return note_detail

            except DataFetchError as ex:
                utils.logger.error(f"[XiaoHongShuCrawler.get_note_detail_async_task] Get note detail error: {ex}")
                return None
            except KeyError as ex:
                utils.logger.error(f"[XiaoHongShuCrawler.get_note_detail_async_task] have not fund note detail note_id:{note_id}, err: {ex}")
                return None

    async def batch_get_note_comments(self, note_list: List[str], xsec_tokens: List[str]):
        """Batch get note comments"""
        if not config.ENABLE_GET_COMMENTS:
            utils.logger.info(f"[XiaoHongShuCrawler.batch_get_note_comments] Crawling comment mode is not enabled")
            return

        utils.logger.info(f"[XiaoHongShuCrawler.batch_get_note_comments] Begin batch get note comments, note list: {note_list}")
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        task_list: List[Task] = []
        for index, note_id in enumerate(note_list):
            task = asyncio.create_task(
                self.get_comments(note_id=note_id, xsec_token=xsec_tokens[index], semaphore=semaphore),
                name=note_id,
            )
            task_list.append(task)

            # Fork自定义: 每处理几个请求后模拟人类行为
            if index % random.randint(3, 5) == 0:
                await self.simulate_human_behavior(self.context_page)

        await asyncio.gather(*task_list)

    async def get_comments(self, note_id: str, xsec_token: str, semaphore: asyncio.Semaphore):
        """Get note comments with keyword filtering and quantity limitation"""
        async with semaphore:
            utils.logger.info(f"[XiaoHongShuCrawler.get_comments] Begin get note id comments {note_id}")
            crawl_interval = config.CRAWLER_MAX_SLEEP_SEC
            await self.xhs_client.get_note_all_comments(
                note_id=note_id,
                xsec_token=xsec_token,
                crawl_interval=crawl_interval,
                callback=xhs_store.batch_update_xhs_note_comments,
                max_count=CRAWLER_MAX_COMMENTS_COUNT_SINGLENOTES,
            )

            await asyncio.sleep(crawl_interval)
            utils.logger.info(f"[XiaoHongShuCrawler.get_comments] Sleeping for {crawl_interval} seconds after fetching comments for note {note_id}")

    async def create_xhs_client(self, httpx_proxy: Optional[str]) -> XiaoHongShuClient:
        """Create xhs client"""
        utils.logger.info("[XiaoHongShuCrawler.create_xhs_client] Begin create xiaohongshu API client ...")
        cookie_str, cookie_dict = utils.convert_cookies(await self.browser_context.cookies())
        xhs_client_obj = XiaoHongShuClient(
            proxy=httpx_proxy,
            headers={
                "accept": "application/json, text/plain, */*",
                "accept-language": "zh-CN,zh;q=0.9",
                "cache-control": "no-cache",
                "content-type": "application/json;charset=UTF-8",
                "origin": "https://www.xiaohongshu.com",
                "pragma": "no-cache",
                "priority": "u=1, i",
                "referer": "https://www.xiaohongshu.com/",
                "sec-ch-ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-site",
                "user-agent": self.user_agent,
                "Cookie": cookie_str,
            },
            playwright_page=self.context_page,
            cookie_dict=cookie_dict,
            proxy_ip_pool=self.ip_proxy_pool,
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
        utils.logger.info("[XiaoHongShuCrawler.launch_browser] Begin create browser context ...")
        if config.SAVE_LOGIN_STATE:
            user_data_dir = os.path.join(os.getcwd(), "browser_data", config.USER_DATA_DIR % config.PLATFORM)
            browser_context = await chromium.launch_persistent_context(
                user_data_dir=user_data_dir,
                accept_downloads=True,
                headless=headless,
                proxy=playwright_proxy,
                viewport={"width": 1920, "height": 1080},
                user_agent=user_agent,
            )
            return browser_context
        else:
            browser = await chromium.launch(headless=headless, proxy=playwright_proxy)
            browser_context = await browser.new_context(viewport={"width": 1920, "height": 1080}, user_agent=user_agent)
            return browser_context

    async def launch_browser_with_cdp(
        self,
        playwright: Playwright,
        playwright_proxy: Optional[Dict],
        user_agent: Optional[str],
        headless: bool = True,
    ) -> BrowserContext:
        """使用CDP模式启动浏览器"""
        try:
            self.cdp_manager = CDPBrowserManager()
            browser_context = await self.cdp_manager.launch_and_connect(
                playwright=playwright,
                playwright_proxy=playwright_proxy,
                user_agent=user_agent,
                headless=headless,
            )

            browser_info = await self.cdp_manager.get_browser_info()
            utils.logger.info(f"[XiaoHongShuCrawler] CDP浏览器信息: {browser_info}")

            return browser_context

        except Exception as e:
            utils.logger.error(f"[XiaoHongShuCrawler] CDP模式启动失败，回退到标准模式: {e}")
            chromium = playwright.chromium
            return await self.launch_browser(chromium, playwright_proxy, user_agent, headless)

    async def close(self):
        """Close browser context"""
        if self.cdp_manager:
            await self.cdp_manager.cleanup()
            self.cdp_manager = None
        else:
            await self.browser_context.close()
        utils.logger.info("[XiaoHongShuCrawler.close] Browser context closed ...")

    async def stop(self):
        """Stop crawler and clean up resources"""
        utils.logger.info("[XiaoHongShuCrawler.stop] Begin stop xiaohongshu crawler ...")
        try:
            if self.cdp_manager:
                await self.cdp_manager.cleanup()
                self.cdp_manager = None
        except Exception as e:
            utils.logger.error(f"[XiaoHongShuCrawler.stop] Error stopping CDP manager: {e}")
        utils.logger.info("[XiaoHongShuCrawler.stop] Stop xiaohongshu crawler successful")

    async def get_notice_media(self, note_detail: Dict):
        if not config.ENABLE_GET_MEIDAS and not getattr(config, 'ENABLE_GET_IMAGES', False):
            utils.logger.info(f"[XiaoHongShuCrawler.get_notice_media] Crawling media mode is not enabled")
            return
        await self.get_note_images(note_detail)
        await self.get_notice_video(note_detail)

    async def get_note_images(self, note_item: Dict):
        """get note images"""
        if not config.ENABLE_GET_MEIDAS and not getattr(config, 'ENABLE_GET_IMAGES', False):
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
            await asyncio.sleep(random.random())
            if content is None:
                continue
            extension_file_name = f"{picNum}.jpg"
            picNum += 1
            await xhs_store.update_xhs_note_image(note_id, content, extension_file_name)

    async def get_notice_video(self, note_item: Dict):
        """get note videos"""
        if not config.ENABLE_GET_MEIDAS and not getattr(config, 'ENABLE_GET_IMAGES', False):
            return
        note_id = note_item.get("note_id")

        videos = xhs_store.get_video_url_arr(note_item)

        if not videos:
            return
        videoNum = 0
        for url in videos:
            content = await self.xhs_client.get_note_media(url)
            await asyncio.sleep(random.random())
            if content is None:
                continue
            extension_file_name = f"{videoNum}.mp4"
            videoNum += 1
            await xhs_store.update_xhs_note_video(note_id, content, extension_file_name)

    # ==================== Fork自定义功能 ====================

    @staticmethod
    def format_proxy_info(kdl_tunnel_proxy: KuaiDaiLiTunnelProxy) -> Tuple[Optional[Dict], Optional[Dict]]:
        """format proxy info for playwright and httpx (KDL隧道代理)"""
        playwright_proxy = {
            "server": kdl_tunnel_proxy.tunnel,
            "username": kdl_tunnel_proxy.user,
            "password": kdl_tunnel_proxy.password,
        }

        httpx_proxy = {
            "http://": f"http://{kdl_tunnel_proxy.user}:{kdl_tunnel_proxy.password}@{kdl_tunnel_proxy.tunnel}",
            "https://": f"http://{kdl_tunnel_proxy.user}:{kdl_tunnel_proxy.password}@{kdl_tunnel_proxy.tunnel}"
        }

        utils.logger.info(f"playwright_proxy: {playwright_proxy}")
        utils.logger.info(f"httpx_proxy: {httpx_proxy}")
        return playwright_proxy, httpx_proxy

    async def simulate_human_behavior(self, page):
        """模拟人类浏览行为"""
        await page.evaluate("""
        () => {
            const scrollHeight = Math.floor(Math.random() * 100);
            window.scrollBy(0, scrollHeight);
        }
        """)

        await page.mouse.move(
            x=random.randint(100, 500),
            y=random.randint(100, 500),
            steps=random.randint(5, 10)
        )

        await asyncio.sleep(random.uniform(0.5, 2.0))

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

        if not value.strip():
            return default

        try:
            if "+" in value:
                value = value.replace("+", "")

            value = value.replace(",", "")

            if value.lower().endswith('k'):
                return int(float(value[:-1]) * 1000)
            elif value.lower().endswith('w'):
                return int(float(value[:-1]) * 10000)
            elif value.lower().endswith('m'):
                return int(float(value[:-1]) * 1000000)

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

    async def rotate_fingerprint(self):
        """定期更换浏览器指纹"""
        await self.browser_context.close()

        new_user_agent = random.choice([
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
        ])

        async with async_playwright() as playwright:
            self.browser_context = await self.launch_browser(
                playwright.chromium,
                None,
                new_user_agent,
                headless=config.HEADLESS
            )

            await self.browser_context.add_init_script(path="libs/stealth.min.js")

            self.context_page = await self.browser_context.new_page()
            await self.context_page.goto(self.index_url)

            await self.xhs_client.update_cookies(browser_context=self.browser_context)
