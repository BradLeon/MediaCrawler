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


# ==================== 基础配置 ====================
PLATFORM = "xhs"  # 平台，xhs | dy | ks | bili | wb | tieba | zhihu
KEYWORDS = "控油水乳,眼霜推荐25-30初抗老,25 岁以上抗初老,钙铁锌哪个牌子好儿童,dha哪个牌子好婴幼儿推荐,事业编备考,成都包包中古店,苏州眉形设计,狗狗牙膏,宠物空气净化器,去猫毛神器,环球影城带娃,川西旅行,丽江旅拍,哈尔滨旅游攻略,云南跟团游,腾冲瑞丽芒市"  # 关键词搜索配置，以英文逗号分隔
LOGIN_TYPE = "qrcode"  # qrcode or phone or cookie
COOKIES = ""

# 具体值参见media_platform.xxx.field下的枚举值
SORT_TYPE = "general"  # 暂时只支持小红书
PUBLISH_TIME_TYPE = 0  # 暂时只支持抖音

CRAWLER_TYPE = (
    "search"  # 爬取类型，search(关键词搜索) | detail(帖子详情)| creator(创作者主页数据)
)

# 自定义User Agent（暂时仅对XHS有效）
UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36'

# ==================== 代理配置 ====================
# 是否开启 IP 代理
ENABLE_IP_PROXY = False

# 代理IP池数量
IP_PROXY_POOL_COUNT = 100

# 代理IP提供商名称
IP_PROXY_PROVIDER_NAME = "kuaidaili"  # kuaidaili | wandouhttp

# ==================== 浏览器配置 ====================
# 设置为True不会打开浏览器（无头浏览器）
# 设置False会打开一个浏览器
# 小红书如果一直扫码登录不通过，打开浏览器手动过一下滑动验证码
# 抖音如果一直提示失败，打开浏览器看下是否扫码登录之后出现了手机号验证，如果出现了手动过一下再试。
HEADLESS = False

# 是否保存登录状态
SAVE_LOGIN_STATE = True

# ==================== CDP (Chrome DevTools Protocol) 配置 ====================
# 是否启用CDP模式 - 使用用户现有的Chrome/Edge浏览器进行爬取，提供更好的反检测能力
# 启用后将自动检测并启动用户的Chrome/Edge浏览器，通过CDP协议进行控制
# 这种方式使用真实的浏览器环境，包括用户的扩展、Cookie和设置，大大降低被检测的风险
ENABLE_CDP_MODE = True

# CDP调试端口，用于与浏览器通信
# 如果端口被占用，系统会自动尝试下一个可用端口
CDP_DEBUG_PORT = 9222

# 自定义浏览器路径（可选）
# 如果为空，系统会自动检测Chrome/Edge的安装路径
# Windows示例: "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"
# macOS示例: "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
CUSTOM_BROWSER_PATH = ""

# CDP模式下是否启用无头模式
# 注意：即使设置为True，某些反检测功能在无头模式下可能效果不佳
CDP_HEADLESS = False

# 浏览器启动超时时间（秒）
BROWSER_LAUNCH_TIMEOUT = 60

# 是否在程序结束时自动关闭浏览器
# 设置为False可以保持浏览器运行，便于调试
AUTO_CLOSE_BROWSER = True

# ==================== 数据存储配置 ====================
# 数据保存类型选项配置,支持五种类型：csv、db、json、sqlite、excel
# 最好保存到DB，有排重的功能。
# 本fork优先使用Supabase作为云数据库
SAVE_DATA_OPTION = "db"  # csv or db or json or sqlite or excel

# 用户浏览器缓存的浏览器文件配置
USER_DATA_DIR = "%s_user_data_dir"  # %s will be replaced by platform name

# ==================== 爬取控制配置 ====================
# 爬取开始页数 默认从第一页开始
START_PAGE = 1

# 爬取视频/帖子的数量控制
CRAWLER_MAX_NOTES_COUNT = 50

# 并发爬虫数量控制
MAX_CONCURRENCY_NUM = 2

# 爬取间隔时间（秒）
CRAWLER_MAX_SLEEP_SEC = 3

# ==================== 媒体爬取配置 ====================
# 是否开启爬媒体模式（包含图片或视频资源），默认不开启爬媒体
ENABLE_GET_MEIDAS = False
# 兼容旧配置名
ENABLE_GET_IMAGES = False

# ==================== 评论爬取配置 ====================
# 是否开启爬评论模式, 默认开启爬评论
ENABLE_GET_COMMENTS = False

# 爬取一级评论的数量控制(单视频/帖子)
CRAWLER_MAX_COMMENTS_COUNT_SINGLENOTES = 100

# 是否开启爬二级评论模式, 默认不开启爬二级评论
# 老版本项目使用了 db, 则需参考 schema/tables.sql line 287 增加表字段
ENABLE_GET_SUB_COMMENTS = True

# ==================== 评论处理配置 (Fork自定义) ====================
# 是否保留评论区对话
ENABLE_COMMENT_CONVERSATION = True
# 评论区对话最大保存轮次
COMMENT_CONVERSATION_MAX_DEPTH = 3
# 处理特定笔记的ID，为空则处理所有笔记
PROCESS_SPECIFIC_NOTE = ""
# 评论语料库输出目录
COMMENT_CORPUS_DIR = "data/corpus"

# ==================== 内容过滤配置 (Fork自定义) ====================
# 帖子最后更新时间阈值 (毫秒时间戳)
# 2024-01-01 00:00:00
LAST_UPDATE_TIME_THRESHOLD = 1704038400000
# 评论数阈值
COMMENT_COUNT_THRESHOLD = 10

# ==================== 词云相关配置 ====================
# 是否开启生成评论词云图
ENABLE_GET_WORDCLOUD = False

# 自定义词语及其分组
# 添加规则：xx:yy 其中xx为自定义添加的词组，yy为将xx该词组分到的组名。
CUSTOM_WORDS = {
    "零几": "年份",  # 将"零几"识别为一个整体
    "高频词": "专业术语",  # 示例自定义词
}

# 停用(禁用)词文件路径
STOP_WORDS_FILE = "./docs/hit_stopwords.txt"

# 中文字体文件路径
FONT_PATH = "./docs/STZHONGS.TTF"

# ==================== Bilibili 特定配置 ====================
# 爬取开始的天数，仅支持 bilibili 关键字搜索，YYYY-MM-DD 格式
START_DAY = '2024-01-01'
# 爬取结束的天数，仅支持 bilibili 关键字搜索，YYYY-MM-DD 格式
END_DAY = '2024-01-01'
# 是否开启按每一天进行爬取的选项，仅支持 bilibili 关键字搜索
ALL_DAY = False

# ==================== 平台特定ID列表配置 ====================
# 指定小红书需要爬虫的笔记URL列表, 目前要携带xsec_token和xsec_source参数
XHS_SPECIFIED_NOTE_URL_LIST = [
    "https://www.xiaohongshu.com/explore/6751582c000000000703a4b5?xsec_token=ABnNAMdt7IJoQfO_vX4E2YzhxDW4XewzgJU1mAUYppOB8=&xsec_source=pc_user"
    # ........................
]

# 指定抖音需要爬取的ID列表
DY_SPECIFIED_ID_LIST = [
    "7280854932641664319",
    "7202432992642387233",
    # ........................
]

# 指定快手平台需要爬取的ID列表
KS_SPECIFIED_ID_LIST = ["3xf8enb8dbj6uig", "3x6zz972bchmvqe"]

# 指定B站平台需要爬取的视频bvid列表
BILI_SPECIFIED_ID_LIST = [
    "BV1d54y1g7db",
    "BV1Sz4y1U77N",
    "BV14Q4y1n7jz",
    # ........................
]

# 指定微博平台需要爬取的帖子列表
WEIBO_SPECIFIED_ID_LIST = [
    "4982041758140155",
    # ........................
]

# 指定weibo创作者ID列表
WEIBO_CREATOR_ID_LIST = [
    "5533390220",
    # ........................
]

# 指定贴吧需要爬取的帖子列表
TIEBA_SPECIFIED_ID_LIST = []

# 指定贴吧名称列表，爬取该贴吧下的帖子
TIEBA_NAME_LIST = [
    # "盗墓笔记"
]

# 指定贴吧创作者URL列表
TIEBA_CREATOR_URL_LIST = [
    "https://tieba.baidu.com/home/main/?id=tb.1.7f139e2e.6CyEwxu3VJruH_-QqpCi6g&fr=frs",
    # ........................
]

# 指定小红书创作者ID列表
XHS_CREATOR_ID_LIST = [
    # 美妆护肤赛道
    # 花西子
    "55d34272f5a26377d1b784dd",
    # 母婴用品赛道
    # SnowBear小白熊
    "5bbafbd598e3dc0001b68aba",
    # Purcotton全棉时代
    "58f6d5d25e87e7083547d51d",
    # EMXEE嫚熙
    "5919e02f50c4b421318e29eb",
    # Hape哈佩
    "5a57013ae8ac2b34fe6611a6",
    # 小鹿蓝蓝
    "5bd326b4f16b990001bc410b"
    # ........................
]

# 指定Dy创作者ID列表(sec_id)
DY_CREATOR_ID_LIST = [
    "MS4wLjABAAAATJPY7LAlaa5X-c8uNdWkvz0jUGgpw4eeXIwu_8BhvqE",
    # ........................
]

# 指定bili创作者ID列表(sec_id)
BILI_CREATOR_ID_LIST = [
    "20813884",
    # ........................
]

# 指定快手创作者ID列表
KS_CREATOR_ID_LIST = [
    "3x4sm73aye7jq7i",
    # ........................
]

# 指定知乎创作者主页url列表
ZHIHU_CREATOR_URL_LIST = [
    "https://www.zhihu.com/people/yd1234567",
    # ........................
]

# 指定知乎需要爬取的帖子ID列表
ZHIHU_SPECIFIED_ID_LIST = [
    "https://www.zhihu.com/question/826896610/answer/4885821440",  # 回答
    "https://zhuanlan.zhihu.com/p/673461588",  # 文章
    "https://www.zhihu.com/zvideo/1539542068422144000"  # 视频
]

# ==================== 导入平台特定配置 ====================
# 注意：这些导入会覆盖上面的同名配置，如果平台配置文件中定义了相同变量
try:
    from .xhs_config import *
except ImportError:
    pass

try:
    from .bilibili_config import *
except ImportError:
    pass

try:
    from .dy_config import *
except ImportError:
    pass

try:
    from .ks_config import *
except ImportError:
    pass

try:
    from .weibo_config import *
except ImportError:
    pass

try:
    from .tieba_config import *
except ImportError:
    pass

try:
    from .zhihu_config import *
except ImportError:
    pass
