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
# @Author  : BradLeon
# @Time    : 2025/4/2 11:32
# @Desc    : 神龙HTTP 代理IP实现
import os
from typing import Dict, List
from urllib.parse import urlencode

import httpx

from proxy import IpCache, IpGetError, ProxyProvider
from proxy.types import IpInfoModel, ProviderNameEnum
from tools import utils


class ShenLongHttpProxy(ProxyProvider):
    def __init__(self, key: str, sign: str, count: int):
        """
        神龙HTTP 代理IP实现
        """
        self.proxy_brand_name = ProviderNameEnum.KUAI_DAILI_PROVIDER.value
        self.api_path = "http://api.shenlongip.com/"
        self.params = {
            "key": key, #产品的key,必须填写
            "sign": sign, # 用户签名,提取时必须添加；
            "count": count, #提取数量, 不能超过200，默认值10；
            "pattern": "json",  # 返回类型，支持txt，json；
            "need": "1101",
            "protocol": "2",
            "mr":"1",
        }
        self.ip_cache = IpCache()

    async def get_proxies(self, num: int) -> List[IpInfoModel]:
        """
        :param num:
        :return:
        """

        # 优先从缓存中拿 IP
        #ip_cache_list = self.ip_cache.load_all_ip(proxy_brand_name=self.proxy_brand_name)
        #if len(ip_cache_list) >= num:
        #    return ip_cache_list[:num]

        # 如果缓存中的数量不够，从IP代理商获取补上，再存入缓存中
        #need_get_count = num - len(ip_cache_list)
        ip_cache_list = []
        ip_infos = []

        async with httpx.AsyncClient() as client:
            url = self.api_path + "/ip" + '?' + urlencode(self.params)
            utils.logger.info(f"[ShenLongHttpProxy.get_proxies] get ip proxy url:{url}")
            response = await client.get(url, headers={
                "User-Agent": "MediaCrawler https://github.com/NanmiCoder/MediaCrawler"})
            res_dict: Dict = response.json()
            if res_dict.get("code") == 200:
                data: List[Dict] = res_dict.get("data")
                current_ts = utils.get_unix_timestamp()
                for ip_item in data:
                    ip_info_model = IpInfoModel(
                        ip=ip_item.get("ip"),
                        port=ip_item.get("port"),
                        prov=ip_item.get("prov"),
                        city=ip_item.get("city"),
                        user="",
                        password="",
                        expired_time_ts=utils.get_unix_time_from_time_str(ip_item.get("expire"))
                    )
                    ip_key = f"SHENLONGHTTP_{ip_info_model.ip}_{ip_info_model.port}"
                    ip_value = ip_info_model.json()
                    ip_infos.append(ip_info_model)
                    self.ip_cache.set_ip(ip_key, ip_value, ex=ip_info_model.expired_time_ts - current_ts)
            else:
                raise IpGetError(res_dict.get("msg", "unkown err"))
        # test
        print("get_shenlong_proxies, ip_infos: ", ip_infos)
        return ip_cache_list + ip_infos


def new_shenlong_http_proxy() -> ShenLongHttpProxy:
    """
    构造神龙HTTP实例
    Returns:

    """
    return ShenLongHttpProxy(
        key=os.getenv("shenlong_proxy_ip_key", ""),  # 通过环境变量的方式获取极速HTTPIP提取key值
        sign=os.getenv("shenlong_proxy_ip_sign", ""),  # 通过环境变量的方式获取极速HTTPIP提取加密签名
        count=2
    )
