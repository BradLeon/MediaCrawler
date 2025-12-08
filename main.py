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
import signal
import sys
from typing import Optional

import cmd_arg
import config
import db
from base.base_crawler import AbstractCrawler
from media_platform.bilibili import BilibiliCrawler
from media_platform.douyin import DouYinCrawler
from media_platform.kuaishou import KuaishouCrawler
from media_platform.tieba import TieBaCrawler
from media_platform.weibo import WeiboCrawler
from media_platform.xhs import XiaoHongShuCrawler
from media_platform.zhihu import ZhihuCrawler
from tools import utils
from var import crawler_type_var


class CrawlerFactory:
    CRAWLERS = {
        "xhs": XiaoHongShuCrawler,
        "dy": DouYinCrawler,
        "ks": KuaishouCrawler,
        "bili": BilibiliCrawler,
        "wb": WeiboCrawler,
        "tieba": TieBaCrawler,
        "zhihu": ZhihuCrawler,
    }

    @staticmethod
    def create_crawler(platform: str) -> AbstractCrawler:
        crawler_class = CrawlerFactory.CRAWLERS.get(platform)
        if not crawler_class:
            raise ValueError(
                "Invalid Media Platform Currently only supported xhs or dy or ks or bili ..."
            )
        return crawler_class()


crawler: Optional[AbstractCrawler] = None


async def main():
    global crawler

    # parse cmd
    args = await cmd_arg.parse_cmd()

    # init db (if using --init_db flag)
    if hasattr(args, 'init_db') and args.init_db:
        await db.init_db(args.init_db)
        print(f"Database {args.init_db} initialized successfully.")
        return

    # Fork: 检查Supabase环境变量（当使用db保存时）
    if config.SAVE_DATA_OPTION == "db":
        supabase_url = os.getenv('SEO_SUPABASE_URL')
        supabase_key = os.getenv('SEO_SUPABASE_ANON_KEY')

        if not supabase_url or not supabase_key:
            utils.logger.error("""
            ==================== Supabase Configuration Error ====================
            When SAVE_DATA_OPTION is set to "db", Supabase environment variables are required.

            Please set the following environment variables:
            - SEO_SUPABASE_URL: Your Supabase project URL
            - SEO_SUPABASE_ANON_KEY: Your Supabase anonymous key

            You can find these values in your Supabase project dashboard:
            1. Go to https://supabase.com/dashboard
            2. Select your project
            3. Go to Settings > API
            4. Copy the URL and anon/public key

            Example:
            export SEO_SUPABASE_URL="https://your-project.supabase.co"
            export SEO_SUPABASE_ANON_KEY="your-anon-key"

            Alternatively, you can change SAVE_DATA_OPTION to "json" or "csv" in config/base_config.py
            =====================================================================
            """)
            sys.exit(1)

        await db.init_db()

    crawler = CrawlerFactory.create_crawler(platform=config.PLATFORM)
    try:
        await crawler.start()
    finally:
        # 确保无论任务是否完成或出现异常，stop方法都会被调用
        if hasattr(crawler, 'stop'):
            await crawler.stop()

    # Flush Excel data if using Excel export
    if config.SAVE_DATA_OPTION == "excel":
        try:
            from store.excel_store_base import ExcelStoreBase
            ExcelStoreBase.flush_all()
            print("[Main] Excel files saved successfully")
        except Exception as e:
            print(f"[Main] Error flushing Excel data: {e}")

    # Generate wordcloud after crawling is complete (JSON mode only)
    if config.SAVE_DATA_OPTION == "json" and config.ENABLE_GET_WORDCLOUD:
        try:
            from tools.async_file_writer import AsyncFileWriter
            file_writer = AsyncFileWriter(
                platform=config.PLATFORM,
                crawler_type=crawler_type_var.get()
            )
            await file_writer.generate_wordcloud_from_comments()
        except Exception as e:
            print(f"Error generating wordcloud: {e}")


async def async_cleanup():
    """异步清理函数，用于处理CDP浏览器等异步资源"""
    global crawler
    if crawler:
        # 检查并清理CDP浏览器
        if hasattr(crawler, 'cdp_manager') and crawler.cdp_manager:
            try:
                await crawler.cdp_manager.cleanup(force=True)
            except Exception as e:
                error_msg = str(e).lower()
                if "closed" not in error_msg and "disconnected" not in error_msg:
                    print(f"[Main] 清理CDP浏览器时出错: {e}")

        # 检查并清理标准浏览器上下文（仅在非CDP模式下）
        elif hasattr(crawler, 'browser_context') and crawler.browser_context:
            try:
                if hasattr(crawler.browser_context, 'pages'):
                    await crawler.browser_context.close()
            except Exception as e:
                error_msg = str(e).lower()
                if "closed" not in error_msg and "disconnected" not in error_msg:
                    print(f"[Main] 关闭浏览器上下文时出错: {e}")

    # 关闭数据库连接
    if config.SAVE_DATA_OPTION in ["db", "sqlite"]:
        await db.close()


def cleanup():
    """同步清理函数"""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(async_cleanup())
        loop.close()
    except Exception as e:
        print(f"[Main] 清理时出错: {e}")


def signal_handler(signum, _frame):
    """信号处理器，处理Ctrl+C等中断信号"""
    print(f"\n[Main] 收到中断信号 {signum}，正在清理资源...")
    cleanup()
    sys.exit(0)


if __name__ == "__main__":
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # 终止信号

    try:
        asyncio.get_event_loop().run_until_complete(main())
    except KeyboardInterrupt:
        print("\n[Main] 收到键盘中断，正在清理资源...")
    finally:
        cleanup()
