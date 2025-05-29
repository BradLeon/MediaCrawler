# Supabase数据库设置指南

本项目已经支持使用Supabase作为数据库存储解决方案，解决了MySQL本地安装和连接的问题。

## 🎯 为什么选择Supabase

- **无需本地安装**：避免MySQL连接错误2003等问题
- **云端托管**：免费额度足够个人项目使用
- **PostgreSQL**：强大的开源数据库
- **实时功能**：支持实时数据同步
- **简单配置**：只需要两个环境变量

## 📋 前置条件

1. 注册Supabase账号：https://supabase.com
2. Python 3.7+
3. 安装依赖：`pip install supabase`

## 🛠️ 设置步骤

### 1. 创建Supabase项目

1. 访问 [Supabase Dashboard](https://supabase.com/dashboard)
2. 点击 "New Project"
3. 选择组织并填写项目信息：
   - Name: `mediacrawler-db` (或你喜欢的名字)
   - Database Password: 设置一个强密码
   - Region: 选择离你最近的区域
4. 等待项目创建完成（通常需要1-2分钟）

### 2. 获取API密钥

1. 在项目dashboard中，点击左侧的 "Settings"
2. 选择 "API"
3. 复制以下信息：
   - **Project URL**（例如：`https://xxxxx.supabase.co`）
   - **anon/public key**（以 `eyJ` 开头的长字符串）

### 3. 设置环境变量

在你的终端中设置环境变量：

```bash
# 临时设置（当前会话有效）
export SEO_SUPABASE_URL="https://your-project-id.supabase.co"
export SEO_SUPABASE_ANON_KEY="your-anon-key-here"

# 永久设置（添加到 ~/.bashrc 或 ~/.zshrc）
echo 'export SEO_SUPABASE_URL="https://your-project-id.supabase.co"' >> ~/.bashrc
echo 'export SEO_SUPABASE_ANON_KEY="your-anon-key-here"' >> ~/.bashrc
source ~/.bashrc
```

### 4. 创建数据表

在Supabase Dashboard中：

1. 点击左侧的 "SQL Editor"
2. 创建新查询
3. 复制并执行以下SQL语句：

```sql
-- 创建xhs_note表
CREATE TABLE IF NOT EXISTS xhs_note (
    note_id VARCHAR(255) PRIMARY KEY,
    type VARCHAR(50),
    title VARCHAR(500),
    desc TEXT,
    video_url TEXT,
    time BIGINT,
    last_update_time BIGINT,
    user_id VARCHAR(255),
    nickname VARCHAR(255),
    avatar VARCHAR(500),
    liked_count INTEGER DEFAULT 0,
    collected_count INTEGER DEFAULT 0,
    comment_count INTEGER DEFAULT 0,
    share_count INTEGER DEFAULT 0,
    ip_location VARCHAR(255),
    image_list TEXT,
    tag_list TEXT,
    last_modify_ts BIGINT,
    note_url TEXT,
    source_keyword VARCHAR(255),
    xsec_token VARCHAR(255),
    add_ts BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建xhs_comment_detail表
CREATE TABLE IF NOT EXISTS xhs_comment_detail (
    comment_id VARCHAR(255) PRIMARY KEY,
    create_time BIGINT,
    ip_location VARCHAR(255),
    note_id VARCHAR(255),
    content TEXT,
    user_id VARCHAR(255),
    nickname VARCHAR(255),
    is_author BOOLEAN DEFAULT FALSE,
    avatar VARCHAR(500),
    sub_comment_count INTEGER DEFAULT 0,
    pictures TEXT,
    parent_comment_id VARCHAR(255) DEFAULT '0',
    last_modify_ts BIGINT,
    like_count INTEGER DEFAULT 0,
    add_ts BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建xhs_author表
CREATE TABLE IF NOT EXISTS xhs_author (
    user_id VARCHAR(255) PRIMARY KEY,
    nickname VARCHAR(255),
    avatar VARCHAR(500),
    desc TEXT,
    gender VARCHAR(10),
    ip_location VARCHAR(255),
    follows INTEGER DEFAULT 0,
    fans INTEGER DEFAULT 0,
    interaction INTEGER DEFAULT 0,
    last_modify_ts BIGINT,
    add_ts BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建搜索结果表
CREATE TABLE IF NOT EXISTS xhs_search_result (
    id SERIAL PRIMARY KEY,
    keyword VARCHAR(255),
    rank INTEGER,
    note_id VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 添加索引以提高查询性能
CREATE INDEX IF NOT EXISTS idx_xhs_note_user_id ON xhs_note(user_id);
CREATE INDEX IF NOT EXISTS idx_xhs_note_source_keyword ON xhs_note(source_keyword);
CREATE INDEX IF NOT EXISTS idx_xhs_comment_note_id ON xhs_comment_detail(note_id);
CREATE INDEX IF NOT EXISTS idx_xhs_comment_parent_id ON xhs_comment_detail(parent_comment_id);
CREATE INDEX IF NOT EXISTS idx_xhs_search_keyword ON xhs_search_result(keyword);
```

### 5. 测试连接

运行测试脚本验证配置：

```bash
python test_supabase_connection.py
```

如果一切正常，你应该看到：
```
🚀 Supabase连接测试开始

🔍 检查环境变量...
✅ SEO_SUPABASE_URL: https://xxxxx.supabase.co
✅ SEO_SUPABASE_ANON_KEY: eyJhbGciOiJIUzI1NiIsI...

🔗 测试Supabase连接...
📋 检查数据库表...
✅ xhs_note 表连接成功
✅ xhs_comment_detail 表连接成功
✅ xhs_author 表连接成功

✅ Supabase连接测试完成！

🏗️  测试数据库初始化...
✅ 数据库初始化成功

🎉 所有测试通过！可以正常使用Supabase数据库存储。
```

## 🎮 使用方法

1. 确保 `config/base_config.py` 中设置：
   ```python
   SAVE_DATA_OPTION = "db"
   ```

2. 运行爬虫：
   ```bash
   python main.py
   ```

项目现在会：
- ✅ 优先使用Supabase存储数据
- ✅ 如果Supabase失败，自动回退到MySQL（如果配置了）
- ✅ 提供详细的错误信息和设置指导

## 🔧 高级配置

### Row Level Security (RLS)

如果需要更好的安全性，可以在Supabase中启用RLS：

```sql
-- 启用RLS
ALTER TABLE xhs_note ENABLE ROW LEVEL SECURITY;
ALTER TABLE xhs_comment_detail ENABLE ROW LEVEL SECURITY;
ALTER TABLE xhs_author ENABLE ROW LEVEL SECURITY;

-- 创建策略（允许所有操作，适用于个人项目）
CREATE POLICY "Allow all operations" ON xhs_note FOR ALL USING (true);
CREATE POLICY "Allow all operations" ON xhs_comment_detail FOR ALL USING (true);
CREATE POLICY "Allow all operations" ON xhs_author FOR ALL USING (true);
```

### 数据迁移

如果你有现有的MySQL数据需要迁移：

1. 使用 `pg_dump` 导出数据
2. 或者写脚本读取MySQL数据并插入到Supabase

## 📊 监控和管理

在Supabase Dashboard中你可以：
- 查看表数据
- 监控API使用情况
- 查看实时日志
- 设置备份策略
- 管理用户权限

## 🚨 故障排除

### 常见问题

1. **环境变量未设置**
   ```
   ❌ SEO_SUPABASE_URL 环境变量未设置
   ```
   解决：按照步骤3重新设置环境变量

2. **API密钥错误**
   ```
   ❌ Supabase客户端初始化失败
   ```
   解决：检查API密钥是否正确复制

3. **表不存在**
   ```
   ⚠️  xhs_note 表可能不存在或无法访问
   ```
   解决：按照步骤4创建数据表

4. **网络连接问题**
   ```
   ❌ Supabase连接失败: connection timeout
   ```
   解决：检查网络连接和防火墙设置

### 调试模式

启用详细日志：
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## 📈 免费额度

Supabase免费额度（每月）：
- 数据库：500MB存储
- API请求：50万次
- 带宽：1GB
- 实时连接：2个并发

对于个人爬虫项目通常足够使用。

## 🔗 有用链接

- [Supabase Documentation](https://supabase.com/docs)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Python Supabase Client](https://github.com/supabase-community/supabase-py)

---

如果遇到问题，请先运行 `python test_supabase_connection.py` 进行诊断，或查看项目日志获取详细错误信息。 