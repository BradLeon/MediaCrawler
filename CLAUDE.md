# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Running the Crawler
```bash
# Basic crawling with keyword search
python main.py --platform xhs --lt qrcode --type search

# Crawl specific content by URL/ID
python main.py --platform dy --lt cookie --type detail

# Crawl creator/user content
python main.py --platform bili --lt phone --type creator

# Initialize database tables
python db.py
```

### Development & Testing
```bash
# Install dependencies
pip install -r requirements.txt

# Run tests
python -m pytest test/

# Run documentation locally
npm run docs:dev

# Build documentation
npm run docs:build
```

### Platform-Specific Parameters
- `--platform`: xhs, dy, ks, bili, wb, tieba, zhihu
- `--lt` (login type): qrcode, phone, cookie
- `--type`: search (keyword), detail (specific content), creator (user profile)

## Architecture

### Core Abstraction Pattern
The codebase uses a consistent abstraction pattern across all platforms:

1. **Base Classes** (`/base/`): Define interfaces that all platforms must implement
   - `AbstractCrawler`: Main crawler interface with search/detail/creator methods
   - `AbstractLogin`: Handles authentication (QR/phone/cookie)
   - `AbstractStore`: Storage abstraction for DB/JSON/CSV
   - `AbstractApiClient`: HTTP client wrapper with rate limiting

2. **Platform Implementations** (`/media_platform/`): Each platform module contains:
   - `client.py`: API client for platform-specific endpoints
   - `core.py`: Main crawler implementation extending AbstractCrawler
   - `login.py`: Platform-specific login logic
   - `field.py`: Data field mappings and transformations
   - `help.py`: Helper functions for that platform

### Data Flow Architecture
```
User Input → Platform Login → Crawler Core → API Client → Data Extraction → Storage Layer
                ↓                                              ↓
           Cookie Cache                                  Comment Processing
                                                               ↓
                                                         Database/Files
```

### Storage Strategy
The system supports multiple storage backends simultaneously:
- **Primary**: Supabase (PostgreSQL) for cloud-based storage
- **Fallback**: Local MySQL for self-hosted deployments
- **Files**: JSON/CSV export for data portability
- **Images**: Downloaded to `data/images/` directory

### Key Design Patterns

1. **Factory Pattern**: Platform crawlers are instantiated via `CrawlerFactory`
2. **Strategy Pattern**: Different login strategies (QR/phone/cookie) per platform
3. **Template Method**: Base crawler defines workflow, platforms implement specifics
4. **Async/Await**: All network operations use asyncio for concurrency
5. **Connection Pooling**: Database and HTTP connections are pooled for efficiency

## Platform-Specific Notes

### XHS (小红书/Xiaohongshu)
- Most mature implementation with comprehensive API coverage
- Supports GraphQL API for efficient data fetching
- Has special handling for note details and creator info
- Uses custom encryption for some API endpoints

### DY (抖音/Douyin)
- Requires special handling for video URLs and aweme IDs
- Implements signature generation for API requests
- Has rate limiting considerations due to strict anti-bot measures

### BILI (B站/Bilibili)
- Supports both video and article content types
- Has special handling for multi-part videos
- Implements wbi signature for certain endpoints

## Database Schema

The system uses a unified schema across all platforms with platform-specific tables:
- `xhs_note`, `dy_aweme`, `bili_video`, etc. - Main content tables
- `xhs_note_comment`, `dy_aweme_comment`, etc. - Comment tables
- `xhs_creator`, `dy_creator`, etc. - User/creator information

Key relationships:
- Comments linked to content via `note_id`/`aweme_id`
- Sub-comments linked to parent comments
- Creators linked to their content

## Configuration System

Primary configuration in `config/base_config.py`:
- `PLATFORM`: Target platform selection
- `CRAWLER_TYPE`: Search/detail/creator mode
- `LOGIN_TYPE`: Authentication method
- `CRAWLER_MAX_NOTES_COUNT`: Crawl limits
- `SAVE_DATA_OPTION`: Storage backend selection
- `ENABLE_IP_PROXY`: Proxy usage toggle

Database configuration in `config/db_config.py` for MySQL and `config/supabase_config.py` for Supabase.

## Anti-Detection Measures

The codebase implements several anti-detection strategies:
1. Browser automation via Playwright (not raw HTTP requests)
2. Realistic user behavior simulation (scrolling, delays)
3. Cookie persistence across sessions
4. User-Agent rotation
5. IP proxy support via proxy pool
6. Rate limiting with configurable delays
7. Session management to avoid frequent re-authentication

## Error Handling

Common error scenarios and handling:
- Network timeouts: Automatic retry with exponential backoff
- Login failures: Falls back to alternative login methods
- Rate limiting: Implements delay and retry logic
- Data parsing errors: Logs and continues with next item
- Database conflicts: Uses upsert operations to handle duplicates

## Legal Compliance

This codebase is designed for educational and research purposes only. When modifying:
- Respect platform Terms of Service
- Implement rate limiting to avoid service disruption
- Do not use for commercial purposes
- Follow robots.txt guidelines
- Ensure data privacy compliance