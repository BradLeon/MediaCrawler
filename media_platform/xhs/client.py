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
import json
import random
import time
import re
from typing import Any, Callable, Dict, List, Optional, Union
from urllib.parse import urlencode
from enum import Enum

import httpx
from playwright.async_api import BrowserContext, Page
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_result, retry_if_exception_type

import config
from base.base_crawler import AbstractApiClient
from tools import utils
from html import unescape

from .exception import DataFetchError, IPBlockError
from .field import SearchNoteType, SearchSortType
from .help import get_search_id, sign

# 导入httpx兼容性工具
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from utils.httpx_compat import create_httpx_async_context


class LoginStatus(Enum):
    """登录状态枚举"""
    OK = "ok"                           # 登录正常
    NEED_LOGIN = "need_login"           # 需要登录
    BLOCKED = "blocked"                 # IP被封禁
    RISK_CONTROL = "risk_control"       # 触发风控，需要等待或验证
    VERIFICATION_NEEDED = "verification_needed"  # 需要验证码


class XiaoHongShuClient(AbstractApiClient):
    def __init__(
        self,
        timeout=20,
        proxies=None,
        *,
        headers: Dict[str, str],
        playwright_page: Page,
        cookie_dict: Dict[str, str],
    ):
        self.proxies = proxies
        self.timeout = timeout
        self.headers = headers
        self._host = "https://edith.xiaohongshu.com"
        self._domain = "https://www.xiaohongshu.com"
        self.IP_ERROR_STR = "网络连接异常，请检查网络设置或重启试试"
        self.IP_ERROR_CODE = 300012
        self.NOTE_ABNORMAL_STR = "笔记状态异常，请稍后查看"
        self.NOTE_ABNORMAL_CODE = -510001
        self.playwright_page = playwright_page
        self.cookie_dict = cookie_dict

    async def _pre_headers(self, url: str, data=None) -> Dict:
        """
        请求头参数签名
        Args:
            url:
            data:

        Returns:

        """
        encrypt_params = await self.playwright_page.evaluate(
            "([url, data]) => window._webmsxyw(url,data)", [url, data]
        )
        local_storage = await self.playwright_page.evaluate("() => window.localStorage")
        signs = sign(
            a1=self.cookie_dict.get("a1", ""),
            b1=local_storage.get("b1", ""),
            x_s=encrypt_params.get("X-s", ""),
            x_t=str(encrypt_params.get("X-t", "")),
        )

        headers = {
            "X-S": signs["x-s"],
            "X-T": signs["x-t"],
            "x-S-Common": signs["x-s-common"],
            "X-B3-Traceid": signs["x-b3-traceid"],
        }
        self.headers.update(headers)
        return self.headers

    @retry(
        stop=stop_after_attempt(5),  # 最多重试5次
        wait=wait_fixed(2),          # 每次重试间隔2秒
        retry=(                      # 定义哪些异常需要重试
            retry_if_exception_type(httpx.TimeoutException) |
            retry_if_exception_type(httpx.ConnectError) |
            retry_if_exception_type(httpx.ProxyError)
        )
    )
    async def request(self, method, url, **kwargs) -> Union[str, Any]:
        """
        封装httpx的公共请求方法，对请求响应做一些处理
        Args:
            method: 请求方法
            url: 请求的URL
            **kwargs: 其他请求参数，例如请求头、请求体等

        Returns:

        """
        return_response = kwargs.pop("return_response", False)
        
        try:
            async with create_httpx_async_context(proxies=self.proxies) as client:
                request_interval = random.uniform(1, 12)  # 随机增加2-7秒
                await asyncio.sleep(request_interval)  # 设置请求时间间隔

                response = await client.request(method, url, timeout=self.timeout, **kwargs)
                
                if response.status_code == 471 or response.status_code == 461:
                    # 处理验证码情况
                    verify_type = response.headers["Verifytype"]
                    verify_uuid = response.headers["Verifyuuid"]
                    raise Exception(
                        f"出现验证码，请求失败，Verifytype: {verify_type}，Verifyuuid: {verify_uuid}, Response: {response}"
                    )
                
                if return_response:
                    return response.text

                data: Dict = response.json()

                # 记录响应的关键信息用于调试
                utils.logger.debug(
                    f"[XiaoHongShuClient.request] Response - "
                    f"status_code: {response.status_code}, "
                    f"success: {data.get('success')}, "
                    f"code: {data.get('code')}, "
                    f"msg: {data.get('msg', '')[:100]}"  # 限制消息长度
                )

                if data["success"]:
                    return data.get("data", data.get("success", {}))
                elif data["code"] == self.IP_ERROR_CODE:
                    utils.logger.error(
                        f"[XiaoHongShuClient.request] IP blocked - "
                        f"code: {data.get('code')}, msg: {data.get('msg')}"
                    )
                    raise IPBlockError(self.IP_ERROR_STR)
                else:
                    utils.logger.error(
                        f"[XiaoHongShuClient.request] Request failed - "
                        f"status_code: {response.status_code}, "
                        f"code: {data.get('code')}, "
                        f"msg: {data.get('msg')}"
                    )
                    raise DataFetchError(data.get("msg", None))
        except httpx.ProxyError as e:
            utils.logger.error(f"代理错误: {e}")
            # 如果是代理身份验证过期，设置标志以便外部刷新代理
            self.proxy_expired = True
            raise  # 重新抛出异常，让tenacity可以进行重试

    async def get(self, uri: str, params=None) -> Dict:
        """
        GET请求，对请求头签名
        Args:
            uri: 请求路由
            params: 请求参数

        Returns:

        """
        final_uri = uri
        if isinstance(params, dict):
            final_uri = f"{uri}?" f"{urlencode(params)}"
        headers = await self._pre_headers(final_uri)
        return await self.request(
            method="GET", url=f"{self._host}{final_uri}", headers=headers
        )

    async def post(self, uri: str, data: dict, **kwargs) -> Dict:
        """
        POST请求，对请求头签名
        Args:
            uri: 请求路由
            data: 请求体参数

        Returns:

        """
        headers = await self._pre_headers(uri, data)
        json_str = json.dumps(data, separators=(",", ":"), ensure_ascii=False)
        # test
        # print("[XiaoHongShuClient.post] url:", f"{self._host}{uri}", "data:", json_str, "headers:", headers)
        return await self.request(
            method="POST",
            url=f"{self._host}{uri}",
            data=json_str,
            headers=headers,
            **kwargs,
        )

    async def get_note_media(self, url: str) -> Union[bytes, None]:
        async with create_httpx_async_context(proxies=self.proxies) as client:
            response = await client.request("GET", url, timeout=self.timeout)
            if not response.reason_phrase == "OK":
                utils.logger.error(
                    f"[XiaoHongShuClient.get_note_media] request {url} err, res:{response.text}"
                )
                return None
            else:
                return response.content

    async def check_login_status(self) -> tuple[LoginStatus, str]:
        """
        检查登录状态，返回详细的状态信息

        Returns:
            tuple[LoginStatus, str]: (状态枚举, 错误消息)
        """
        utils.logger.info("[XiaoHongShuClient.check_login_status] Checking login status...")

        try:
            note_card: Dict = await self.get_note_by_keyword(keyword="美妆OOTD")

            # 打印返回的关键信息用于调试
            items_count = len(note_card.get("items", []))
            has_more = note_card.get("has_more", False)
            utils.logger.info(
                f"[XiaoHongShuClient.check_login_status] Response received - "
                f"items_count: {items_count}, has_more: {has_more}, "
                f"keys: {list(note_card.keys())}"
            )

            if note_card.get("items"):
                utils.logger.info("[XiaoHongShuClient.check_login_status] Login status: OK")
                return LoginStatus.OK, ""
            else:
                # 返回了数据但没有items，可能需要登录
                utils.logger.warning(
                    f"[XiaoHongShuClient.check_login_status] No items in response, may need login. "
                    f"Response keys: {list(note_card.keys())}"
                )
                return LoginStatus.NEED_LOGIN, "No items in search result"

        except IPBlockError as e:
            # IP被封禁
            utils.logger.error(f"[XiaoHongShuClient.check_login_status] IP blocked: {e}")
            return LoginStatus.BLOCKED, str(e)

        except DataFetchError as e:
            error_msg = str(e)
            utils.logger.error(f"[XiaoHongShuClient.check_login_status] Data fetch error: {error_msg}")

            # 根据错误消息判断具体情况
            if "账号异常" in error_msg or "稍后重试" in error_msg or "稍后重启" in error_msg:
                # 风控相关错误
                return LoginStatus.RISK_CONTROL, error_msg
            elif "验证码" in error_msg or "验证" in error_msg:
                # 需要验证码
                return LoginStatus.VERIFICATION_NEEDED, error_msg
            elif "登录" in error_msg or "权限" in error_msg:
                # 需要登录
                return LoginStatus.NEED_LOGIN, error_msg
            else:
                # 其他数据获取错误，可能是登录问题
                return LoginStatus.NEED_LOGIN, error_msg

        except Exception as e:
            # 其他异常，保守处理，认为可能需要登录
            utils.logger.error(f"[XiaoHongShuClient.check_login_status] Unexpected error: {e}")
            return LoginStatus.NEED_LOGIN, str(e)

    async def pong(self) -> bool:
        """
        用于检查登录态是否失效了（向后兼容的简化版本）

        Returns:
            bool: True=登录正常, False=需要登录

        Note: 此方法为向后兼容保留，推荐使用 check_login_status() 获取详细状态
        """
        utils.logger.info("[XiaoHongShuClient.pong] Begin to pong xhs...")

        status, error_msg = await self.check_login_status()

        if status == LoginStatus.OK:
            return True
        elif status == LoginStatus.RISK_CONTROL:
            # 风控情况：cookies有效但被限制，不需要重新登录
            utils.logger.warning(
                f"[XiaoHongShuClient.pong] Risk control detected: {error_msg}. "
                "Cookies are still valid, no need to re-login. Please wait or verify manually."
            )
            return True  # 返回True，避免触发重新登录
        elif status == LoginStatus.BLOCKED:
            # IP被封，cookies可能有效但IP有问题
            utils.logger.error(f"[XiaoHongShuClient.pong] IP blocked: {error_msg}")
            return True  # 返回True，因为重新登录也解决不了IP问题
        else:
            # 其他情况需要登录
            utils.logger.error(f"[XiaoHongShuClient.pong] Login required: {error_msg}")
            return False


    async def update_cookies(self, browser_context: BrowserContext):
        """
        API客户端提供的更新cookies方法，一般情况下登录成功后会调用此方法
        Args:
            browser_context: 浏览器上下文对象

        Returns:

        """
        cookie_str, cookie_dict = utils.convert_cookies(await browser_context.cookies())
        self.headers["Cookie"] = cookie_str
        self.cookie_dict = cookie_dict

    async def get_note_by_keyword(
        self,
        keyword: str,
        search_id: str = get_search_id(),
        page: int = 1,
        page_size: int = 20,
        sort: SearchSortType = SearchSortType.GENERAL,
        note_type: SearchNoteType = SearchNoteType.ALL,
    ) -> Dict:
        """
        根据关键词搜索笔记
        Args:
            keyword: 关键词参数
            page: 分页第几页
            page_size: 分页数据长度
            sort: 搜索结果排序指定
            note_type: 搜索的笔记类型

        Returns:

        """
        uri = "/api/sns/web/v1/search/notes"
        data = {
            "keyword": keyword,
            "page": page,
            "page_size": page_size,
            "search_id": search_id,
            "sort": sort.value,
            "note_type": note_type.value,
        }
        return await self.post(uri, data)

    async def get_note_by_id(
        self, note_id: str, xsec_source: str, xsec_token: str
    ) -> Dict:
        """
        获取笔记详情API
        Args:
            note_id:笔记ID
            xsec_source: 渠道来源
            xsec_token: 搜索关键字之后返回的比较列表中返回的token

        Returns:

        """
        if xsec_source == "":
            xsec_source = "pc_search"

        data = {
            "source_note_id": note_id,
            "image_formats": ["jpg", "webp", "avif"],
            "extra": {"need_body_topic": 1},
            "xsec_source": xsec_source,
            "xsec_token": xsec_token,
        }
        uri = "/api/sns/web/v1/feed"
        res = await self.post(uri, data)
        if res and res.get("items"):
            res_dict: Dict = res["items"][0]["note_card"]
            return res_dict
        # 爬取频繁了可能会出现有的笔记能有结果有的没有
        utils.logger.error(
            f"[XiaoHongShuClient.get_note_by_id] get note id:{note_id} empty and res:{res}"
        )
        return dict()

    async def get_note_comments(
        self, note_id: str, xsec_token: str, cursor: str = ""
    ) -> Dict:
        """
        获取一级评论的API
        Args:
            note_id: 笔记ID
            xsec_token: 验证token
            cursor: 分页游标

        Returns:

        """
        uri = "/api/sns/web/v2/comment/page"
        params = {
            "note_id": note_id,
            "cursor": cursor,
            "top_comment_id": "",
            "image_formats": "jpg,webp,avif",
            "xsec_token": xsec_token,
        }
        return await self.get(uri, params)

    async def get_note_sub_comments(
        self,
        note_id: str,
        root_comment_id: str,
        xsec_token: str,
        num: int = 10,
        cursor: str = "",
    ):
        """
        获取指定父评论下的子评论的API
        Args:
            note_id: 子评论的帖子ID
            root_comment_id: 根评论ID
            xsec_token: 验证token
            num: 分页数量
            cursor: 分页游标

        Returns:

        """
        uri = "/api/sns/web/v2/comment/sub/page"
        params = {
            "note_id": note_id,
            "root_comment_id": root_comment_id,
            "num": num,
            "cursor": cursor,
            "image_formats": "jpg,webp,avif",
            "top_comment_id": "",
            "xsec_token": xsec_token,
        }
        return await self.get(uri, params)
    

    async def get_note_all_comments(
        self,
        note_id: str,
        xsec_token: str,
        crawl_interval: float = 1.0,
        callback: Optional[Callable] = None,
        max_count: int = 10,
    ) -> List[Dict]:
        """
        获取指定笔记下的所有一级评论，该方法会一直查找一个帖子下的所有评论信息
        Args:
            note_id: 笔记ID
            xsec_token: 验证token
            crawl_interval: 爬取一次笔记的延迟单位（秒）
            callback: 一次笔记爬取结束后
            max_count: 一次笔记爬取的最大评论数量
        Returns:

        """
        result = []
        comments_has_more = True
        comments_cursor = ""
        while comments_has_more and len(result) < max_count:

            request_interval = random.uniform(8, 15)  # 随机增加2-7秒
            await asyncio.sleep(request_interval)  # 设置请求时间间隔
        
            comments_res = await self.get_note_comments(
                note_id=note_id, xsec_token=xsec_token, cursor=comments_cursor
            )
            comments_has_more = comments_res.get("has_more", False)
            comments_cursor = comments_res.get("cursor", "")
            if "comments" not in comments_res:
                utils.logger.info(
                    f"[XiaoHongShuClient.get_note_all_comments] No 'comments' key found in response: {comments_res}"
                )
                break
            comments = comments_res["comments"]
            if len(result) + len(comments) > max_count:
                comments = comments[: max_count - len(result)]
            if callback:
                await callback(note_id, comments)
            await asyncio.sleep(crawl_interval)
            result.extend(comments)
            sub_comments = await self.get_comments_all_sub_comments(
                comments=comments,
                xsec_token=xsec_token,
                crawl_interval=crawl_interval,
                callback=callback,
            )
            result.extend(sub_comments)
        return result
    

    async def get_creator_info(self, user_id: str) -> Dict:
        """
        通过解析网页版的用户主页HTML，获取用户个人简要信息
        PC端用户主页的网页存在window.__INITIAL_STATE__这个变量上的，解析它即可
        eg: https://www.xiaohongshu.com/user/profile/59d8cb33de5fb4696bf17217
        """
        uri = f"/user/profile/{user_id}"
        html_content = await self.request(
            "GET", self._domain + uri, return_response=True, headers=self.headers
        )
        match = re.search(
            r"<script>window.__INITIAL_STATE__=(.+)<\/script>", html_content, re.M
        )

    async def get_current_user_info(self) -> Dict:
        """
        获取当前登录用户信息
        Returns:
            用户信息字典，包含昵称、用户名、手机号等信息
        """
        uri = "/api/sns/web/v1/user/selfinfo"
        try:
            # 尝试使用新的API
            return await self.get(uri)
        except Exception as e:
            utils.logger.warning(f"[XiaoHongShuClient.get_current_user_info] Primary API failed: {e}, trying alternative...")
            # 如果主要API失败，尝试备用API        
            return {
                    "nickname": "unknown_user",
                    "username": "unknown",
                    "phone": "unknown",
                    "user_id": "unknown",
                    "error": f"Failed to get user info: {e}"
                }

    async def get_current_user_nickname(self) -> str:
        current_user_info = await self.get_current_user_info()
        try:
            current_user_nickname = current_user_info.get("basic_info").get("nickname")
            return current_user_nickname
        except Exception as e:
            utils.logger.error(f"[XiaoHongShuClient.get_current_user_nickname] Error getting current user nickname: {e}")
            return "unknown_user"

    async def get_notes_by_creator(
        self, creator: str, cursor: str, page_size: int = 30
    ) -> Dict:
        """
        获取博主的笔记
        Args:
            creator: 博主ID
            cursor: 上一页最后一条笔记的ID
            page_size: 分页数据长度

        Returns:

        """
        uri = "/api/sns/web/v1/user_posted"
        data = {
            "user_id": creator,
            "cursor": cursor,
            "num": page_size,
            "image_formats": "jpg,webp,avif",
        }
        return await self.get(uri, data)

    async def get_all_notes_by_creator(
        self,
        user_id: str,
        crawl_interval: float = 1.0,
        max_count:  int = 300,
        callback: Optional[Callable] = None,
    ) -> List[Dict]:
        """
        获取指定用户下的所有发过的帖子，该方法会一直查找一个用户下的所有帖子信息
        Args:
            user_id: 用户ID
            crawl_interval: 爬取一次的延迟单位（秒）
            callback: 一次分页爬取结束后的更新回调函数

        Returns:

        """
        result = []
        notes_has_more = True
        notes_cursor = ""
        while notes_has_more:
            notes_res = await self.get_notes_by_creator(user_id, notes_cursor)

            request_interval = random.uniform(5, 30)  # 随机增加2-7秒
            await asyncio.sleep(request_interval)  # 设置请求时间间隔

            if not notes_res:
                utils.logger.error(
                    f"[XiaoHongShuClient.get_notes_by_creator] The current creator may have been banned by xhs, so they cannot access the data."
                )
                break

            notes_has_more = notes_res.get("has_more", False)
            notes_cursor = notes_res.get("cursor", "")
            if "notes" not in notes_res:
                utils.logger.info(
                    f"[XiaoHongShuClient.get_all_notes_by_creator] No 'notes' key found in response: {notes_res}"
                )
                break

            notes = notes_res["notes"]
            utils.logger.info(
                f"[XiaoHongShuClient.get_all_notes_by_creator] got user_id:{user_id} notes len : {len(notes)}"
            )
            if callback:
                await callback(notes)
            await asyncio.sleep(crawl_interval)  # 爬取间隔
            result.extend(notes)

            if len(result) >= max_count:
                return result
            
        return result
    
    async def get_note_short_url(self, note_id: str) -> Dict:
        """
        获取笔记的短链接
        Args:
            note_id: 笔记ID

        Returns:

        """
        uri = f"/api/sns/web/short_url"
        data = {"original_url": f"{self._domain}/discovery/item/{note_id}"}
        return await self.post(uri, data=data, return_response=True)

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(1))
    async def get_note_by_id_from_html(
        self,
        note_id: str,
        xsec_source: str,
        xsec_token: str,
        enable_cookie: bool = False,
    ) -> Optional[Dict]:
        """
        通过解析网页版的笔记详情页HTML，获取笔记详情, 该接口可能会出现失败的情况，这里尝试重试3次
        copy from https://github.com/ReaJason/xhs/blob/eb1c5a0213f6fbb592f0a2897ee552847c69ea2d/xhs/core.py#L217-L259
        thanks for ReaJason
        Args:
            note_id:
            xsec_source:
            xsec_token:
            enable_cookie:

        Returns:

        """

        def camel_to_underscore(key):
            return re.sub(r"(?<!^)(?=[A-Z])", "_", key).lower()

        def transform_json_keys(json_data):
            data_dict = json.loads(json_data)
            dict_new = {}
            for key, value in data_dict.items():
                new_key = camel_to_underscore(key)
                if not value:
                    dict_new[new_key] = value
                elif isinstance(value, dict):
                    dict_new[new_key] = transform_json_keys(json.dumps(value))
                elif isinstance(value, list):
                    dict_new[new_key] = [
                        (
                            transform_json_keys(json.dumps(item))
                            if (item and isinstance(item, dict))
                            else item
                        )
                        for item in value
                    ]
                else:
                    dict_new[new_key] = value
            return dict_new

        url = (
            "https://www.xiaohongshu.com/explore/"
            + note_id
            + f"?xsec_token={xsec_token}&xsec_source={xsec_source}"
        )
    
        copy_headers = self.headers.copy()
        if not enable_cookie:
            del copy_headers["Cookie"]

        html = await self.request(
            method="GET", url=url, return_response=True, headers=copy_headers
        )

        # test
        #print("[XiaoHongShuClient.get_note_by_id_from_html] enable_cookie:", enable_cookie)
        #print("[XiaoHongShuClient.get_note_by_id_from_html] url:", url, "headers:", copy_headers)

        def get_note_dict(html):
            state = re.findall(r"window.__INITIAL_STATE__=({.*})</script>", html)[
                0
            ].replace("undefined", '""')

            if state != "{}":
                note_dict = transform_json_keys(state)
                return note_dict["note"]["note_detail_map"][note_id]["note"]
            return {}

        try:
            return get_note_dict(html)
        except:
            return None

    async def get_comments_all_sub_comments(
        self,
        comments: List[Dict],
        xsec_token: str,
        crawl_interval: float = 1.0,
        callback: Optional[Callable] = None,
        max_depth: int = config.COMMENT_CONVERSATION_MAX_DEPTH,  # 默认最大递归深度为3
        current_depth: int = 1  # 当前递归深度，初始为1
    ) -> List[Dict]:
        """
        递归获取多级子评论，可以获取任意深度的评论树
        
        Note: 这个方法是get_comments_all_sub_comments的增强版，可以获取多级嵌套的子评论
        
        Args:
            comments: 评论列表
            xsec_token: 验证token
            crawl_interval: 爬取一次评论的延迟单位（秒）
            callback: 一次评论爬取结束后的回调函数
            max_depth: 最大递归深度，默认为3，设置为-1表示无限递归
            current_depth: 当前递归深度，仅内部使用
            
        Returns:
            所有子评论的列表，包含嵌套结构
        """
        if not config.ENABLE_GET_SUB_COMMENTS:
            utils.logger.info(
                f"[XiaoHongShuClient.get_comments_all_sub_comments] Crawling sub_comment mode is not enabled"
            )
            return []
        
        # 检查是否达到最大递归深度
        if max_depth > 0 and current_depth > max_depth:
            return []
        
        result = []
        for comment in comments:
            note_id = comment.get("note_id")
            
            # 处理已存在的子评论
            sub_comments = comment.get("sub_comments", [])
            if sub_comments and callback:
                await callback(note_id, sub_comments)
                result.extend(sub_comments)
            
            # 检查是否有更多子评论需要获取
            sub_comment_has_more = comment.get("sub_comment_has_more", False)
            if not sub_comment_has_more:
                continue
            
            root_comment_id = comment.get("id")
            sub_comment_cursor = comment.get("sub_comment_cursor", "")
            
            # 获取所有子评论
            child_comments = []
            while sub_comment_has_more:
                request_interval = random.uniform(7, 15)  # 随机增加2-7秒
                await asyncio.sleep(request_interval)  # 设置请求时间间隔
                try:
                    comments_res = await self.get_note_sub_comments(
                        note_id=note_id,
                        root_comment_id=root_comment_id,
                        xsec_token=xsec_token,
                        num=20,
                        cursor=sub_comment_cursor,
                    )
                    
                    sub_comment_has_more = comments_res.get("has_more", False)
                    sub_comment_cursor = comments_res.get("cursor", "")
                    
                    if "comments" not in comments_res:
                        utils.logger.info(
                            f"[XiaoHongShuClient.get_comments_all_sub_comments] No 'comments' key found in response: {comments_res}"
                        )
                        break
                    
                    fetched_comments = comments_res["comments"]
                    if not fetched_comments:
                        break
                    
                    # 处理这一批子评论
                    if callback:
                        await callback(note_id, fetched_comments)
                    
                    child_comments.extend(fetched_comments)
                    await asyncio.sleep(crawl_interval)
                    
                except Exception as e:
                    utils.logger.error(f"[XiaoHongShuClient.get_comments_all_sub_comments] Error fetching sub comments: {e}")
                    await asyncio.sleep(crawl_interval)
            
            # 将这些子评论添加到结果中
            result.extend(child_comments)
            
            # 递归获取更深层级的子评论（如果需要）
            if max_depth < 0 or current_depth < max_depth:
                # 为每个子评论设置note_id
                for child in child_comments:
                    if "note_id" not in child:
                        child["note_id"] = note_id
                
                # 递归调用，获取下一级子评论
                deeper_comments = await self.get_comments_all_sub_comments(
                    comments=child_comments,
                    xsec_token=xsec_token,
                    crawl_interval=crawl_interval,
                    callback=callback,
                    max_depth=max_depth,
                    current_depth=current_depth + 1
                )
                
                # 更新每个子评论的sub_comments字段
                comment_map = {comment["id"]: comment for comment in child_comments if "id" in comment}
                
                # 按父评论ID分组
                child_comment_groups = {}
                for sub_comment in deeper_comments:
                    if "target_comment" in sub_comment and "id" in sub_comment["target_comment"]:
                        parent_id = sub_comment["target_comment"]["id"]
                        if parent_id not in child_comment_groups:
                            child_comment_groups[parent_id] = []
                        child_comment_groups[parent_id].append(sub_comment)
                
                # 将子评论添加到对应的父评论中
                for parent_id, children in child_comment_groups.items():
                    if parent_id in comment_map:
                        if "sub_comments" not in comment_map[parent_id]:
                            comment_map[parent_id]["sub_comments"] = []
                        comment_map[parent_id]["sub_comments"].extend(children)
                
                # 将更深层级的子评论添加到结果中
                result.extend(deeper_comments)
        
        return result
