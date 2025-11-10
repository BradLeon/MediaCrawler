# -*- coding: utf-8 -*-
# 声明：本代码仅供学习和研究目的使用。使用者应遵守以下原则：
# 1. 遵守目标平台的使用条款和robots.txt规则。
# 2. 确保数据采集活动符合当地法律法规。
# 3. 尊重数据隐私，不采集个人敏感信息。
# 4. 合理控制采集频率，避免对目标平台造成不必要的负担。
# 5. 不得将采集的数据用于任何非法或不道德的用途。
#
# 作者对因使用本代码而产生的任何法律责任不承担责任。
# 使用本代码即表示您同意上述声明。

"""
小红书签名生成适配器
支持混合模式：优先使用xhshow纯Python签名，失败时降级到浏览器签名
"""

import time
from typing import Any, Dict, Optional
from enum import Enum

from tools import utils

# 可选导入xhshow（如果未安装不影响浏览器模式）
try:
    from xhshow import Xhshow
    XHSHOW_AVAILABLE = True
except ImportError:
    XHSHOW_AVAILABLE = False
    utils.logger.warning("[XHSSignAdapter] xhshow not installed, will use browser-only mode")


class SignMethod(Enum):
    """签名生成方法枚举"""
    XHSHOW = "xhshow"      # 纯Python签名（xhshow库）
    BROWSER = "browser"    # 浏览器签名（window.mnsv2）
    HYBRID = "hybrid"      # 混合模式（优先xhshow，失败降级browser）


class XHSSignAdapter:
    """小红书签名生成适配器 - 支持多种签名方法的统一接口"""

    def __init__(
        self,
        method: str = "hybrid",
        playwright_page=None,
        cookie_dict: Optional[Dict[str, str]] = None,
        timeout: int = 5
    ):
        """
        初始化签名适配器

        Args:
            method: 签名方法 ("xhshow", "browser", "hybrid")
            playwright_page: Playwright页面对象（browser/hybrid模式需要）
            cookie_dict: Cookie字典（包含a1等）
            timeout: xhshow超时时间（秒）
        """
        self.method = SignMethod(method)
        self.playwright_page = playwright_page
        self.cookie_dict = cookie_dict or {}
        self.timeout = timeout

        # xhshow客户端（懒加载）
        self._xhshow_client: Optional[Xhshow] = None

        # 统计指标
        self.metrics = {
            "xhshow_attempts": 0,
            "xhshow_success": 0,
            "xhshow_failures": 0,
            "browser_attempts": 0,
            "browser_success": 0,
            "browser_failures": 0,
            "fallback_count": 0,
        }

        # 验证配置
        self._validate_config()

    def _validate_config(self):
        """验证配置是否满足选定签名方法的要求"""
        if self.method in (SignMethod.BROWSER, SignMethod.HYBRID):
            if not self.playwright_page:
                raise ValueError(
                    f"Playwright page is required for {self.method.value} mode"
                )

        if self.method == SignMethod.XHSHOW and not XHSHOW_AVAILABLE:
            raise ValueError(
                "xhshow is not installed. Please run: pip install xhshow"
            )

    @property
    def xhshow_client(self) -> Xhshow:
        """懒加载xhshow客户端"""
        if not XHSHOW_AVAILABLE:
            raise RuntimeError("xhshow is not available")

        if self._xhshow_client is None:
            self._xhshow_client = Xhshow()
            utils.logger.info("[XHSSignAdapter] Initialized xhshow client")

        return self._xhshow_client

    async def generate_signature(
        self,
        uri: str,
        data: Optional[Any] = None
    ) -> str:
        """
        生成x-s签名（统一接口）

        Args:
            uri: 请求URI或完整URL
            data: 请求数据（GET为params dict，POST为payload dict）

        Returns:
            签名字符串（XYS_开头）

        Raises:
            Exception: 所有签名方法都失败时抛出异常
        """
        if self.method == SignMethod.XHSHOW:
            return await self._xhshow_sign(uri, data)
        elif self.method == SignMethod.BROWSER:
            return await self._browser_sign(uri, data)
        elif self.method == SignMethod.HYBRID:
            return await self._hybrid_sign(uri, data)
        else:
            raise ValueError(f"Unknown sign method: {self.method}")

    async def _xhshow_sign(self, uri: str, data: Optional[Any]) -> str:
        """
        使用xhshow生成签名

        Args:
            uri: 请求URI或完整URL
            data: 请求数据

        Returns:
            签名字符串
        """
        self.metrics["xhshow_attempts"] += 1

        try:
            start_time = time.time()

            # 获取a1 cookie
            a1_value = self.cookie_dict.get("a1", "")
            if not a1_value:
                raise ValueError("a1 cookie is required for xhshow signature")

            # 根据data类型判断是GET还是POST
            if data is None or isinstance(data, dict) and all(
                isinstance(v, (str, int, float, bool, type(None))) for v in data.values()
            ):
                # GET请求（data为params或None）
                signature = self.xhshow_client.sign_xs_get(
                    uri=uri,
                    a1_value=a1_value,
                    params=data or {}
                )
            else:
                # POST请求（data为payload）
                signature = self.xhshow_client.sign_xs_post(
                    uri=uri,
                    a1_value=a1_value,
                    payload=data
                )

            elapsed_time = time.time() - start_time
            self.metrics["xhshow_success"] += 1

            utils.logger.info(
                f"[XHSSignAdapter] xhshow signature generated successfully "
                f"(time: {elapsed_time:.3f}s)"
            )

            return signature

        except Exception as e:
            self.metrics["xhshow_failures"] += 1
            utils.logger.error(
                f"[XHSSignAdapter] xhshow signature failed: {e}"
            )
            raise

    async def _browser_sign(self, uri: str, data: Optional[Any]) -> str:
        """
        使用浏览器生成签名（window.mnsv2）

        Args:
            uri: 请求URI
            data: 请求数据

        Returns:
            签名字符串
        """
        self.metrics["browser_attempts"] += 1

        try:
            start_time = time.time()

            # 导入浏览器签名函数
            from .secsign import seccore_signv2_playwright

            signature = await seccore_signv2_playwright(
                self.playwright_page,
                uri,
                data
            )

            elapsed_time = time.time() - start_time
            self.metrics["browser_success"] += 1

            utils.logger.info(
                f"[XHSSignAdapter] Browser signature generated successfully "
                f"(time: {elapsed_time:.3f}s)"
            )

            return signature

        except Exception as e:
            self.metrics["browser_failures"] += 1
            utils.logger.error(
                f"[XHSSignAdapter] Browser signature failed: {e}"
            )
            raise

    async def _hybrid_sign(self, uri: str, data: Optional[Any]) -> str:
        """
        混合模式签名：优先xhshow，失败时降级到browser

        Args:
            uri: 请求URI
            data: 请求数据

        Returns:
            签名字符串
        """
        # 优先尝试xhshow
        if XHSHOW_AVAILABLE:
            try:
                utils.logger.debug(
                    "[XHSSignAdapter] Attempting xhshow signature (hybrid mode)"
                )
                return await self._xhshow_sign(uri, data)
            except Exception as e:
                utils.logger.warning(
                    f"[XHSSignAdapter] xhshow failed in hybrid mode: {e}, "
                    "falling back to browser"
                )
                self.metrics["fallback_count"] += 1
        else:
            utils.logger.info(
                "[XHSSignAdapter] xhshow not available, using browser directly"
            )

        # 降级到浏览器签名
        utils.logger.info("[XHSSignAdapter] Using browser signature (fallback)")
        return await self._browser_sign(uri, data)

    def get_metrics(self) -> Dict[str, Any]:
        """
        获取签名生成统计指标

        Returns:
            统计指标字典
        """
        total_attempts = (
            self.metrics["xhshow_attempts"] +
            self.metrics["browser_attempts"]
        )

        if total_attempts == 0:
            return {**self.metrics, "xhshow_success_rate": 0.0}

        xhshow_success_rate = (
            self.metrics["xhshow_success"] / self.metrics["xhshow_attempts"]
            if self.metrics["xhshow_attempts"] > 0
            else 0.0
        )

        return {
            **self.metrics,
            "total_attempts": total_attempts,
            "xhshow_success_rate": xhshow_success_rate,
        }

    def reset_metrics(self):
        """重置统计指标"""
        for key in self.metrics:
            self.metrics[key] = 0
        utils.logger.info("[XHSSignAdapter] Metrics reset")
