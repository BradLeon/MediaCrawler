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

import hashlib
import base64
import json
from typing import Any


def _build_c(e: Any, a: Any) -> str:
    """
    拼接签名所需的原文
    e: url
    a: payload
    """
    c = str(e)
    if isinstance(a, (dict, list)):
        c += json.dumps(a, separators=(",", ":"), ensure_ascii=False)
    elif isinstance(a, str):
        c += a
    return c


def _md5_hex(s: str) -> str:
    """对字符串进行 MD5 哈希"""
    return hashlib.md5(s.encode("utf-8")).hexdigest()


async def seccore_signv2_playwright(
    page,  # Playwright Page
    e: Any,
    a: Any,
) -> str:
    """
    使用 Playwright 调用 window.mnsv2(c, d) 来生成签名

    Args:
        page: Playwright page object
        e: url
        a: payload

    Returns:
        签名 token (XYS_开头的 base64 字符串)
    """
    c = _build_c(e, a)
    d = _md5_hex(c)

    # 调用浏览器上下文里的 window.mnsv2
    s = await page.evaluate("(c, d) => window.mnsv2(c, d)", [c, d])
    f = {
        "x0": "4.2.6",
        "x1": "xhs-pc-web",
        "x2": "Mac OS",
        "x3": s,
        "x4": a,
    }
    payload = json.dumps(f, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    token = "XYS_" + base64.b64encode(payload).decode("ascii")
    print(token)
    return token
