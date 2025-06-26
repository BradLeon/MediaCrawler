# 声明：本代码仅供学习和研究目的使用。使用者应遵守以下原则：  
# 1. 不得用于任何商业用途。  
# 2. 使用时应遵守目标平台的使用条款和robots.txt规则。  
# 3. 不得进行大规模爬取或对平台造成运营干扰。  
# 4. 应合理控制请求频率，避免给目标平台带来不必要的负担。   
# 5. 不得用于任何非法或不当的用途。
#   
# 详细许可条款请参阅项目根目录下的LICENSE文件。  
# 使用本代码即表示您同意遵守上述原则和LICENSE中的所有条款。  


import argparse

import config
from tools.utils import str2bool


async def parse_cmd():
    # 读取command arg
    parser = argparse.ArgumentParser(description='Media crawler program.')
    parser.add_argument('--platform', type=str, help='Media platform select (xhs | dy | ks | bili | wb | tieba | zhihu)',
                        choices=["xhs", "dy", "ks", "bili", "wb", "tieba", "zhihu"], default=config.PLATFORM)
    parser.add_argument('--lt', type=str, help='Login type (qrcode | phone | cookie)',
                        choices=["qrcode", "phone", "cookie"], default=config.LOGIN_TYPE)
    parser.add_argument('--type', type=str, help='crawler type (search | detail | creator)',
                        choices=["search", "detail", "creator"], default=config.CRAWLER_TYPE)
    parser.add_argument('--start', type=int,
                        help='number of start page', default=config.START_PAGE)
    parser.add_argument('--keywords', type=str,
                        help='please input keywords', default=config.KEYWORDS)
    parser.add_argument('--get_comment', type=str2bool,
                        help='''whether to crawl level one comment, supported values case insensitive ('yes', 'true', 't', 'y', '1', 'no', 'false', 'f', 'n', '0')''', default=config.ENABLE_GET_COMMENTS)
    parser.add_argument('--get_sub_comment', type=str2bool,
                        help=''''whether to crawl level two comment, supported values case insensitive ('yes', 'true', 't', 'y', '1', 'no', 'false', 'f', 'n', '0')''', default=config.ENABLE_GET_SUB_COMMENTS)
    parser.add_argument('--save_data_option', type=str,
                        help='where to save the data (csv or db or json)', choices=['csv', 'db', 'json'], default=config.SAVE_DATA_OPTION)
    parser.add_argument('--cookies', type=str,
                        help='cookies used for cookie login type', default=config.COOKIES)
    
    # 新增的详情采集相关参数
    parser.add_argument('--max_count', type=int,
                        help='maximum number of notes/videos to crawl', default=config.CRAWLER_MAX_NOTES_COUNT)
    parser.add_argument('--max_comments', type=int,
                        help='maximum number of comments per note/video', default=config.CRAWLER_MAX_COMMENTS_COUNT_SINGLENOTES)
    parser.add_argument('--headless', type=str2bool,
                        help='whether to run browser in headless mode', default=config.HEADLESS)
    parser.add_argument('--enable_proxy', type=str2bool,
                        help='whether to enable IP proxy', default=config.ENABLE_IP_PROXY)
    
    # 小红书特定参数
    parser.add_argument('--xhs_note_urls', type=str,
                        help='XHS note URLs separated by semicolon for detail crawling', default=None)
    
    # 抖音特定参数  
    parser.add_argument('--dy_ids', type=str,
                        help='Douyin video IDs separated by semicolon for detail crawling', default=None)
    
    # 快手特定参数
    parser.add_argument('--ks_ids', type=str,
                        help='Kuaishou video IDs separated by semicolon for detail crawling', default=None)
    
    # B站特定参数
    parser.add_argument('--bili_ids', type=str,
                        help='Bilibili video BVIDs separated by semicolon for detail crawling', default=None)
    
    # 微博特定参数
    parser.add_argument('--weibo_ids', type=str,
                        help='Weibo post IDs separated by semicolon for detail crawling', default=None)
    
    # 知乎特定参数
    parser.add_argument('--zhihu_urls', type=str,
                        help='Zhihu URLs separated by semicolon for detail crawling', default=None)
    
    # 创作者模式参数
    parser.add_argument('--xhs_creator_ids', type=str,
                        help='XHS creator IDs separated by semicolon for creator crawling', default=None)
    parser.add_argument('--dy_creator_ids', type=str,
                        help='Douyin creator IDs separated by semicolon for creator crawling', default=None)
    parser.add_argument('--ks_creator_ids', type=str,
                        help='Kuaishou creator IDs separated by semicolon for creator crawling', default=None)
    parser.add_argument('--bili_creator_ids', type=str,
                        help='Bilibili creator IDs separated by semicolon for creator crawling', default=None)
    parser.add_argument('--weibo_creator_ids', type=str,
                        help='Weibo creator IDs separated by semicolon for creator crawling', default=None)
    parser.add_argument('--zhihu_creator_urls', type=str,
                        help='Zhihu creator URLs separated by semicolon for creator crawling', default=None)
    parser.add_argument('--tieba_creator_urls', type=str,
                        help='Tieba creator URLs separated by semicolon for creator crawling', default=None)

    args = parser.parse_args()

    # override config
    config.PLATFORM = args.platform
    config.LOGIN_TYPE = args.lt
    config.CRAWLER_TYPE = args.type
    config.START_PAGE = args.start
    config.KEYWORDS = args.keywords
    config.ENABLE_GET_COMMENTS = args.get_comment
    config.ENABLE_GET_SUB_COMMENTS = args.get_sub_comment
    config.SAVE_DATA_OPTION = args.save_data_option
    config.COOKIES = args.cookies
    
    # 新增配置项覆盖
    config.CRAWLER_MAX_NOTES_COUNT = args.max_count
    config.CRAWLER_MAX_COMMENTS_COUNT_SINGLENOTES = args.max_comments
    config.HEADLESS = args.headless
    config.ENABLE_IP_PROXY = args.enable_proxy
    
    # 平台特定的ID/URL列表处理 - Detail模式
    if args.xhs_note_urls:
        config.XHS_SPECIFIED_NOTE_URL_LIST = args.xhs_note_urls.split(';')
    
    if args.dy_ids:
        config.DY_SPECIFIED_ID_LIST = args.dy_ids.split(';')
        
    if args.ks_ids:
        config.KS_SPECIFIED_ID_LIST = args.ks_ids.split(';')
        
    if args.bili_ids:
        config.BILI_SPECIFIED_ID_LIST = args.bili_ids.split(';')
        
    if args.weibo_ids:
        config.WEIBO_SPECIFIED_ID_LIST = args.weibo_ids.split(';')
        
    if args.zhihu_urls:
        config.ZHIHU_SPECIFIED_ID_LIST = args.zhihu_urls.split(';')
    
    # 平台特定的创作者ID/URL列表处理 - Creator模式
    if args.xhs_creator_ids:
        config.XHS_CREATOR_ID_LIST = args.xhs_creator_ids.split(';')
    
    if args.dy_creator_ids:
        config.DY_CREATOR_ID_LIST = args.dy_creator_ids.split(';')
        
    if args.ks_creator_ids:
        config.KS_CREATOR_ID_LIST = args.ks_creator_ids.split(';')
        
    if args.bili_creator_ids:
        config.BILI_CREATOR_ID_LIST = args.bili_creator_ids.split(';')
        
    if args.weibo_creator_ids:
        config.WEIBO_CREATOR_ID_LIST = args.weibo_creator_ids.split(';')
        
    if args.zhihu_creator_urls:
        config.ZHIHU_CREATOR_URL_LIST = args.zhihu_creator_urls.split(';')
        
    if args.tieba_creator_urls:
        config.TIEBA_CREATOR_URL_LIST = args.tieba_creator_urls.split(';')
