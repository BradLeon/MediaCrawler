# Project Context

## Purpose
MediaCrawler is an open-source social media crawler for Chinese platforms, designed for **educational purposes** and **research**. It enables structured data extraction from:

- **Xiaohongshu (XHS)** - Little Red Book
- **Douyin (DY)** - Chinese TikTok
- **Kuaishou (KS)**
- **Bilibili (Bili)**
- **Weibo (WB)**
- **Baidu Tieba**
- **Zhihu**

The project emphasizes learning web scraping techniques, architecture patterns, and ethical data collection practices.

## Tech Stack

### Core
- **Language**: Python 3.9 (strict requirement)
- **Async Framework**: asyncio (async/await throughout)
- **Browser Automation**: Playwright 1.42.0 (Chromium)
- **HTTP Client**: httpx 0.27.0

### Data Management
- **Primary Database**: MySQL via aiomysql 0.2.0
- **Alternative Database**: Supabase (PostgreSQL cloud)
- **Caching**: Redis 4.6.0 or in-memory (ExpiringLocalCache)
- **Data Validation**: Pydantic 2.5.2

### Data Processing
- **DataFrames**: Pandas 2.2.3
- **Text Segmentation**: jieba 0.42.1 (Chinese tokenization)
- **Visualization**: wordcloud, matplotlib

### API & Web
- **Framework**: FastAPI 0.110.2
- **Server**: uvicorn 0.29.0

## Project Conventions

### Code Style
- **Language**: Chinese comments and docstrings
- **Encoding**: UTF-8 with explicit declarations
- **Modules**: snake_case (`base_crawler.py`)
- **Classes**: PascalCase (`XiaoHongShuCrawler`)
- **Functions**: snake_case (`store_content()`)
- **Constants**: UPPER_SNAKE_CASE (`ENABLE_IP_PROXY`)
- **Platform Codes**: `xhs`, `dy`, `ks`, `bili`, `wb`, `tieba`, `zhihu`
- **Type Hints**: Required for function signatures
- **Legal Header**: Every Python file starts with 5-principle disclaimer

### Architecture Patterns

**Layered Architecture:**
```
Entry Point (main.py)
    ↓
Command Parser (cmd_arg/)
    ↓
Crawler Factory → Platform Crawlers (media_platform/{platform}/)
    ↓
Storage Layer (store/{platform}/) → CSV / Database / JSON
    ↓
Infrastructure (proxy/, cache/, async_db.py)
```

**Key Patterns:**
- **Factory Pattern**: `CrawlerFactory`, `XhsStoreFactory`, `CacheFactory`
- **Abstract Base Classes**: `AbstractCrawler`, `AbstractLogin`, `AbstractStore`, `AbstractApiClient`
- **Strategy Pattern**: Multiple storage backends, proxy providers, login methods
- **Context Variables**: `contextvars` for async-safe state (`crawler_type_var`, `media_crawler_db_var`)

**Platform Structure** (each in `media_platform/{platform}/`):
- `core.py` - Crawler implementation
- `client.py` - API client with request signing
- `login.py` - Authentication handler
- `field.py` - Enums and type definitions
- `help.py` - Platform helpers

### Testing Strategy
- **Location**: `/test/` directory
- **Coverage**: Utility functions, cache implementations, proxy pool
- **Files**: `test_utils.py`, `test_expiring_local_cache.py`, `test_redis_cache.py`, `test_proxy_ip_pool.py`
- **Gaps**: Limited platform-specific tests, no formal test runner configuration

### Git Workflow
- **Main Branch**: `main`
- **Commit Style**: Descriptive messages in Chinese or English
- **PR Requirements**: Code review before merge

## Domain Context

### Crawling Types
- **Search**: Keyword-based content discovery
- **Detail**: Fetch full content by URL/ID
- **Creator**: Collect creator profiles and their content

### Data Models
- **Notes/Videos**: Platform content with engagement metrics
- **Comments**: Primary and secondary (nested) comments
- **Creators**: User profiles with follower counts

### Authentication Flow
1. Launch Playwright browser with persistent context
2. Login via QR code, phone number, or saved cookies
3. Capture authentication tokens/cookies
4. Sign HTTP requests with platform-specific signatures

### Anti-Bot Handling
- Playwright stealth mode (`stealth.min.js`)
- Request rate limiting (`CRAWLER_MAX_SLEEP_SEC`)
- Concurrency limits (`MAX_CONCURRENCY_NUM = 2`)
- CAPTCHA handling (manual intervention for slider)

## Important Constraints

### Technical
- **Python 3.9 only** - Strict version requirement
- **Playwright required** - Must run `playwright install` for browser binaries
- **Node.js >= 16** - Required for Douyin and Zhihu (JavaScript execution)
- **Concurrency limit** - Default 2 concurrent requests to avoid detection

### Legal/Ethical (5 Principles)
1. Educational and research use only
2. Respect platform Terms of Service
3. Follow robots.txt restrictions
4. Do not cause operational disruption
5. No large-scale commercial scraping

### Data
- Platform-specific data structures (XHS notes ≠ Douyin videos)
- Comment recursion (primary → secondary)
- Session persistence via browser profiles

## External Dependencies

### Social Media Platforms
- Direct API access with signed requests
- Platform-specific authentication flows
- Dynamic signature generation (JavaScript execution via pyexecjs)

### Proxy Services (Optional)
- KuaiDaiLi (tunnel and API)
- Shenlong proxy
- Jishu HTTP proxy

### Databases
- **MySQL**: Default storage at localhost:3306
- **Supabase**: Alternative PostgreSQL cloud (see SUPABASE_SETUP.md)
- **Redis**: Optional caching at localhost:6379

### Browser
- Chromium via Playwright (auto-downloaded)
- Persistent browser contexts in `browser_data/`
