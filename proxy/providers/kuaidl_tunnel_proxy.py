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
   
        self.user = kdl_user_name
        self.password = kdl_user_pwd
        self.tunnel = "t308.kdltpspro.com:15818"
        self.secret_id = kdl_secret_id
        self.signature = kdl_signature