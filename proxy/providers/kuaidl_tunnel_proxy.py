import os
import re
from typing import Dict, List

import httpx

class KuaiDaiLiTunnelProxy():
    def __init__(self):
        """
            快代理隧道代理实现
        Args:
            kdl_user_name:
            kdl_user_pwd:
        """
        kdl_user_name = os.getenv("kdl_tunnel_proxy_user_name", "你的快代理用户名")
        kdl_user_pwd = os.getenv("kdl_tunnel_proxy_pwd", "你的快代理密码")
        kdl_secret_id = os.getenv("kdl_tunnel_proxy_secret_id", "你的快代理secert_id")
        kdl_signature = os.getenv("kdl_tunnel_proxy_signature", "你的快代理签名")
   
        self.user = 't15266542856997'
        self.password = 'wv0r9fwg'
        self.tunnel = "n531.kdltps.com:15818"
        self.secret_id = 'o8z4qhird66tzl8wd1l3'
        self.signature = 'rrpd3u5kd7s28i6ngv07ein5nfcecsxx'
