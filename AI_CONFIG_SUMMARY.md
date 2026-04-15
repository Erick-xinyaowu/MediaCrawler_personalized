# 抖音 AI 主题爬虫配置总结

## ✅ 已完成的配置修改

### 1. 基础配置 (`config/base_config.py`)
- **PLATFORM**: `dy` (抖音平台)
- **KEYWORDS**: `AI` (搜索关键词)
- **LOGIN_TYPE**: `qrcode` (二维码登录)
- **CRAWLER_TYPE**: `search` (关键词搜索模式)
- **ENABLE_GET_COMMENTS**: `True` (启用评论爬取)
- **CRAWLER_MAX_NOTES_COUNT**: `15` (最多爬取15条视频)
- **CRAWLER_MAX_COMMENTS_COUNT_SINGLENOTES**: `10` (每个视频最多爬取10条评论)

### 2. 字段过滤 (`store/douyin/__init__.py`)

#### 视频字段 (保留11个字段)
```
aweme_id         - 视频ID
title            - 视频标题
desc             - 视频描述
create_time      - 发布时间
user_id          - 作者ID
nickname         - 作者昵称
liked_count      - 点赞数
collected_count  - 收藏数
comment_count    - 评论数
share_count      - 分享数
ip_location      - 发布地点
```

#### 评论字段 (保留8个字段)
```
comment_id       - 评论ID
aweme_id         - 视频ID
content          - 评论内容
create_time      - 评论时间
user_id          - 评论者ID
like_count       - 评论点赞数
sub_comment_count - 子评论数
ip_location      - 评论地点
```

## 🚀 运行爬虫

### 方式1：直接命令行运行
```bash
# 进入项目目录
cd E:\Mediainfo\MediaCrawler

# 确保PATH中有uv
$env:PATH = $env:PATH + ";$env:USERPROFILE\.local\bin"

# 运行爬虫
uv run main.py --platform dy --lt qrcode --type search
```

### 方式2：使用WebUI界面
```bash
# 启动Web服务（http://localhost:8080）
$env:PATH = $env:PATH + ";$env:USERPROFILE\.local\bin"
uv run uvicorn api.main:app --port 8080 --reload
```

## 📁 输出数据

### 存储位置
- 默认保存位置: `data/douyin` 文件夹
- 格式: `jsonl`（每行一个JSON对象）

### 数据结构示例

**视频数据** (videos.jsonl)
```json
{
  "aweme_id": "7525538910311632128",
  "title": "AI编程教程分享",
  "desc": "今天分享一些AI编程的技巧...",
  "create_time": 1704067200,
  "user_id": "123456789",
  "nickname": "技术UP主",
  "liked_count": "1024",
  "collected_count": "256",
  "comment_count": "128",
  "share_count": "64",
  "ip_location": "北京"
}
```

**评论数据** (comments.jsonl)
```json
{
  "comment_id": "7525538910311632128",
  "aweme_id": "7525538910311632128",
  "content": "这个教程很实用！",
  "create_time": 1704067200,
  "user_id": "987654321",
  "like_count": 10,
  "sub_comment_count": "2",
  "ip_location": "上海"
}
```

## ⚠️ 注意事项

1. **登录**: 首次运行会弹出二维码，需要用抖音App扫码登录
2. **风控防护**: 程序已配置为仅涉及爬取，建议：
   - 每个视频间隔爬取 `CRAWLER_MAX_SLEEP_SEC = 2` 秒
   - 并发数设置为 `MAX_CONCURRENCY_NUM = 1`
3. **反检测**: 已启用 CDP 模式，使用真实浏览器进行爬取
4. **数据清洁**: 所有返回的数据都已过滤为指定字段
5. **爬虫政策**: 仅供学习用途，请遵守平台政策

## 📊 爬虫状态验证

运行爬虫时，终端会显示类似信息：
```
[store.douyin.update_douyin_aweme] douyin aweme id:7525538910311632128, title:AI编程教程分享
[store.douyin.update_dy_aweme_comment] douyin aweme comment: xxx, content: 这个教程很实用！
```

## 🛠️ 自定义修改

如需修改：
- **搜索关键词**: 编辑 `config/base_config.py` 的 `KEYWORDS`
- **爬取数量**: 编辑 `CRAWLER_MAX_NOTES_COUNT` 和 `CRAWLER_MAX_COMMENTS_COUNT_SINGLENOTES`
- **字段列表**: 编辑 `store/douyin/__init__.py` 中的 `save_content_item` 和 `save_comment_item` 字典

---
**创建时间**: 2026年4月15日
**配置版本**: v1.0
