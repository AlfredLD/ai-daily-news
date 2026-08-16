# -*- coding: utf-8 -*-
"""
每日 AI 资讯日报 - GitHub Actions 版 v5
========================================
GitHub Actions 定时触发, 每天 09:00(北京时间)执行:
  1. 采集过去 24 小时 AI 领域资讯(30 个信息源, 覆盖国内外)
  2. 调用 LLM API 做中文摘要、去重、重要性分级(主模型+备用模型自动切换)
     —— 强制全简体中文输出(仅公司名/模型名/参数保留原文), 每条必出"关注理由"
  3. 生成标准 Markdown 日报(四大板块, 每板块≤20条, 按优先级排序, 高优先级🔴前缀)
  4. 通过飞书群机器人 Webhook 以【单张交互卡片】推送完整文档(不再分片为多条消息)
  5. 日报同时归档为 reports/AI资讯日报_YYYY-MM-DD.md 并提交到仓库
  6. 真正采集失败的源等待 30 分钟后重采, 推送"补充更新"

信息源分层(30 个):
  - 官方博客(official, 9): OpenAI / Anthropic / Google DeepMind / Google
    Research / Meta AI / Microsoft AI / Mistral AI / NVIDIA / Hugging Face
  - 国际权威媒体(media, 8): TechCrunch / VentureBeat / The Verge /
    MIT Tech Review / Ars Technica / WIRED / The Decoder / MarkTechPost
  - 国内权威媒体(media, 4): 机器之心 / 量子位 / 36氪 / 雷锋网
  - 聚合源(aggregate, 5): Google News 中英文检索(覆盖 DeepSeek/xAI/
    Moonshot/智谱等无官方 RSS 的公司, 以及融资并购动态)
  - 学术与社区(API, 4): arXiv / Hugging Face 模型榜 / GitHub / Hacker News

可靠性设计(三重防线):
  1. 每个源配置多个候选 URL(官方 RSS + RSSHub 镜像), 依次尝试
  2. 全部候选失败时, 自动降级为 Google News RSS 检索间接获取,
     条目标注"间接获取", 不再计入"采集失败"
  3. 连 Google News 也失败才计为失败源, 30 分钟后重采并推送补充更新
  另: 指数退避重试(5s/10s/20s)、末轮全量重试、UA 与 Accept-Language 伪装

部署: GitHub 公开仓库 → .github/workflows/daily-ai-report.yml
入口: python ai_daily_report.py

环境变量(通过 GitHub Secrets 配置):
  FEISHU_WEBHOOK       必填  飞书群机器人 Webhook 地址
  FEISHU_SECRET        选填  飞书群机器人加签密钥
  LLM_API_KEY          必填  LLM 服务 API Key(主模型)
  LLM_BASE_URL         选填  默认 https://api.deepseek.com/v1
  LLM_MODEL            选填  默认 deepseek-chat
  LLM_BACKUP_API_KEY   选填  备用 LLM API Key(主模型失败时切换)
  LLM_BACKUP_BASE_URL  选填  备用 LLM Base URL
  LLM_BACKUP_MODEL     选填  备用 LLM 模型名
  GITHUB_TOKEN         自动  GitHub Actions 自动提供, 提高 API 限额
  REPORT_ARCHIVE       选填  默认 true, 归档日报到 reports/ 目录
"""

import base64
import hashlib
import hmac
import json
import os
import re
import subprocess
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import quote

import requests

# ---------------------------------------------------------------------------
# 配置(从环境变量读取)
# ---------------------------------------------------------------------------
FEISHU_WEBHOOK = os.environ.get("FEISHU_WEBHOOK", "").strip()
FEISHU_SECRET = os.environ.get("FEISHU_SECRET", "").strip()
LLM_API_KEY = os.environ.get("LLM_API_KEY", "").strip()
# 注意: workflow 通过 secrets 传值时, 未配置的 secret 会展开为空字符串,
# 空串会覆盖默认值导致 LLM 请求失败, 故用 `or` 回退到默认值
LLM_BASE_URL = (os.environ.get("LLM_BASE_URL", "") or "https://api.deepseek.com/v1").strip().rstrip("/")
LLM_MODEL = (os.environ.get("LLM_MODEL", "") or "deepseek-chat").strip()
LLM_BACKUP_API_KEY = os.environ.get("LLM_BACKUP_API_KEY", "").strip()
LLM_BACKUP_BASE_URL = os.environ.get("LLM_BACKUP_BASE_URL", "").strip().rstrip("/")
LLM_BACKUP_MODEL = os.environ.get("LLM_BACKUP_MODEL", "").strip()
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()
REPORT_ARCHIVE = os.environ.get("REPORT_ARCHIVE", "true").strip().lower() != "false"

# 采集参数
MAX_ITEMS_PER_SOURCE = 10         # 每个源最多采集条数
MAX_ITEMS_AGGREGATE = 12          # 聚合源最多采集条数
REQUEST_TIMEOUT = 15              # 网络请求超时(秒)
RETRY_TIMES = 3                   # 每个请求重试次数(指数退避 5s/10s/20s)
FINAL_RETRY_DELAY = 15            # 末轮全量重试前等待(秒)
HN_STORY_LIMIT = 80               # Hacker News 拉取条目数
RETRY_WAIT_SECONDS = 1800         # 失败源重采等待时间(30分钟)

# 飞书卡片限制: 单个 markdown 元素内容安全上限(字符)
CARD_CHUNK_CHARS = 3600
# 每个板块最多保留条数(用户要求各≤20)
MAX_PER_CATEGORY = 20
# 单张卡片内容上限(飞书交互卡片 markdown 元素约 30KB, 留余量)
CARD_CONTENT_LIMIT = 29000

# AI 关键词(用于综合媒体过滤)
AI_KEYWORDS = [
    "ai", "artificial intelligence", "llm", "gpt", "chatgpt", "openai",
    "anthropic", "claude", "deepseek", "gemini", "bard", "midjourney",
    "stable diffusion", "diffusion", "transformer", "neural", "machine learning",
    "deep learning", "agent", "copilot", "mistral", "llama", "qwen", "glm",
    "sora", "runway", "perplexity", "grok", "xai", "deepmind", "kimi",
    "大模型", "人工智能", "智能体", "机器学习", "深度学习", "算力",
    "多模态", "自动驾驶", "具身智能", "AGI", "千问", "文心", "混元", "豆包",
]

# 通用请求头(避免被部分网站拦截)
COMMON_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "application/rss+xml, application/xml, application/json, "
              "text/xml, text/html, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

# ---------------------------------------------------------------------------
# 信息源配置
# 每个 RSS 源: name / urls(候选URL列表) / need_filter / stype /
#              news_query + news_lang(Google News 间接获取降级用的检索词)
# URLs 实测校验于 2026-08-16:
#   - Anthropic/Meta/Mistral 无官方 RSS(404/400) → 走 RSSHub 与 Google News 降级
#   - Microsoft /ai/feed 已 410 Gone → 改用主博客 feed + AI 关键词过滤
#   - Google Research/Blogspot 需海外网络(GitHub runner 可直达)
# ---------------------------------------------------------------------------
RSSHUB = "https://rsshub.app"

SOURCES = [
    # ===== 国际官方博客 (权威性最高) =====
    dict(name="OpenAI Blog", stype="official", need_filter=False,
         urls=["https://openai.com/news/rss.xml",
               "https://openai.com/blog/rss.xml",
               f"{RSSHUB}/openai/blog"],
         news_query="OpenAI", news_lang="en"),
    dict(name="Anthropic Blog", stype="official", need_filter=False,
         urls=[f"{RSSHUB}/anthropic/news",
               "https://www.anthropic.com/news/rss.xml",
               "https://www.anthropic.com/rss.xml"],
         news_query="Anthropic Claude", news_lang="en"),
    dict(name="Google DeepMind Blog", stype="official", need_filter=False,
         urls=["https://deepmind.google/blog/rss.xml"],
         news_query="DeepMind", news_lang="en"),
    dict(name="Google Research Blog", stype="official", need_filter=False,
         urls=["https://research.google/blog/rss/",
               "https://googleaiblog.blogspot.com/feeds/posts/default"],
         news_query="Google AI Gemini", news_lang="en"),
    dict(name="Meta AI Blog", stype="official", need_filter=False,
         urls=["https://ai.meta.com/blog/rss/",
               f"{RSSHUB}/meta/ai"],
         news_query="Meta AI Llama", news_lang="en"),
    dict(name="Microsoft AI Blog", stype="official", need_filter=True,
         urls=["https://blogs.microsoft.com/feed/"],
         news_query="Microsoft Copilot AI", news_lang="en"),
    dict(name="Mistral AI Blog", stype="official", need_filter=False,
         urls=[f"{RSSHUB}/mistral/news",
               "https://mistral.ai/news/rss.xml"],
         news_query="Mistral AI", news_lang="en"),
    dict(name="NVIDIA Blog", stype="official", need_filter=True,
         urls=["https://blogs.nvidia.com/feed/"],
         news_query="NVIDIA AI", news_lang="en"),
    dict(name="Hugging Face Blog", stype="official", need_filter=False,
         urls=["https://huggingface.co/blog/feed.xml"],
         news_query="Hugging Face", news_lang="en"),

    # ===== 国际权威科技媒体 =====
    dict(name="TechCrunch AI", stype="media", need_filter=False,
         urls=["https://techcrunch.com/category/artificial-intelligence/feed/"],
         news_query="TechCrunch AI", news_lang="en"),
    dict(name="VentureBeat AI", stype="media", need_filter=False,
         urls=["https://venturebeat.com/category/ai/feed/"],
         news_query="VentureBeat AI", news_lang="en"),
    dict(name="The Verge AI", stype="media", need_filter=False,
         urls=["https://www.theverge.com/rss/ai-artificial-intelligence/index.xml"],
         news_query="The Verge AI", news_lang="en"),
    dict(name="MIT Tech Review AI", stype="media", need_filter=False,
         urls=["https://www.technologyreview.com/topic/artificial-intelligence/feed/"],
         news_query="MIT Technology Review AI", news_lang="en"),
    dict(name="Ars Technica AI", stype="media", need_filter=False,
         urls=["https://arstechnica.com/ai/feed/"],
         news_query=None, news_lang="en"),
    dict(name="WIRED AI", stype="media", need_filter=False,
         urls=["https://www.wired.com/feed/tag/ai/latest/rss"],
         news_query=None, news_lang="en"),
    dict(name="The Decoder", stype="media", need_filter=False,
         urls=["https://the-decoder.com/feed/"],
         news_query=None, news_lang="en"),
    dict(name="MarkTechPost", stype="media", need_filter=False,
         urls=["https://www.marktechpost.com/feed/"],
         news_query=None, news_lang="en"),

    # ===== 国内权威媒体 =====
    dict(name="机器之心", stype="media", need_filter=False,
         urls=["https://www.jiqizhixin.com/rss"],
         news_query="机器之心 大模型", news_lang="zh"),
    dict(name="量子位", stype="media", need_filter=False,
         urls=["https://www.qbitai.com/feed"],
         news_query="量子位 AI", news_lang="zh"),
    dict(name="36氪", stype="media", need_filter=True,
         urls=["https://36kr.com/feed"],
         news_query="36氪 AI 大模型", news_lang="zh"),
    dict(name="雷锋网", stype="media", need_filter=True,
         urls=["https://www.leiphone.com/feed",
               f"{RSSHUB}/leiphone"],
         news_query="雷锋网 AI", news_lang="zh"),

    # ===== Google News 聚合源 (独立采集, 覆盖无 RSS 的公司与主题) =====
    dict(name="Google新闻·海外AI公司", stype="aggregate", need_filter=False,
         urls=[lambda: google_news_url(
             "OpenAI OR Anthropic OR DeepSeek OR Mistral OR xAI Grok OR Perplexity", "en")],
         news_query=None, news_lang="en"),
    dict(name="Google新闻·国内大模型", stype="aggregate", need_filter=False,
         urls=[lambda: google_news_url(
             "DeepSeek OR Kimi OR 智谱 OR 豆包 OR 通义千问 OR 文心一言", "zh")],
         news_query=None, news_lang="zh"),
    dict(name="Google新闻·AI大模型", stype="aggregate", need_filter=False,
         urls=[lambda: google_news_url("大模型 OR 人工智能 OR AGI", "zh")],
         news_query=None, news_lang="zh"),
    dict(name="Google新闻·芯片算力", stype="aggregate", need_filter=False,
         urls=[lambda: google_news_url("AI芯片 OR 算力 OR 英伟达", "zh")],
         news_query=None, news_lang="zh"),
    dict(name="Google新闻·AI投融资", stype="aggregate", need_filter=False,
         urls=[lambda: google_news_url("AI 融资 OR 收购 OR 估值", "zh")],
         news_query=None, news_lang="zh"),
]


def google_news_url(query, lang="zh"):
    """构造 Google News RSS 检索 URL。lang: zh=中文区, en=英文区"""
    when = "when:2d" if lang == "zh" else "when:1d"
    q = quote(f"{query} {when}")
    if lang == "zh":
        return (f"https://news.google.com/rss/search?q={q}"
                f"&hl=zh-CN&gl=CN&ceid=CN:zh-Hans")
    return (f"https://news.google.com/rss/search?q={q}"
            f"&hl=en-US&gl=US&ceid=US:en")


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------
def beijing_now():
    """返回北京时间(UTC+8)当前时间(无时区信息)"""
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=8)


def log(msg):
    print(f"[{beijing_now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def _mask_key(k):
    """脱敏显示 API Key(前4后4, 中间打码)"""
    if not k:
        return "(未配置)"
    if len(k) <= 8:
        return "***"
    return k[:4] + "****" + k[-4:]


def _clean_html(text):
    """清洗 HTML 标签与纯链接, 压缩空白, 返回纯文本(可能为空串)"""
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)          # 去 HTML 标签
    text = re.sub(r"https?://\S+", "", text)       # 去纯链接
    text = re.sub(r"&nbsp;|&amp;|&lt;|&gt;|&#\d+;", " ", text)  # 去实体
    return re.sub(r"\s+", " ", text).strip()


def fetch_with_retry(url, headers=None, params=None, timeout=REQUEST_TIMEOUT,
                     retries=RETRY_TIMES):
    """带指数退避重试的 GET 请求, 成功返回 requests.Response, 失败返回 None"""
    final_headers = dict(COMMON_HEADERS)
    if headers:
        final_headers.update(headers)
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, headers=final_headers, params=params, timeout=timeout)
            if resp.status_code == 200:
                return resp
            log(f"  HTTP {resp.status_code}: {url[:100]}")
        except Exception as e:
            log(f"  请求异常({attempt + 1}/{retries + 1}): {str(e)[:120]}")
        if attempt < retries:
            time.sleep(min(5 * (2 ** attempt), 20))  # 5s -> 10s -> 20s
    return None


def parse_rss_date(date_str):
    """解析 RSS/Atom 日期字符串, 返回 naive datetime(已转北京时间 UTC+8)"""
    if not date_str:
        return None
    date_str = date_str.strip()

    # RFC 822 格式 (RSS 2.0): "Wed, 15 Aug 2026 09:30:00 +0000"
    try:
        dt = parsedate_to_datetime(date_str)
        if dt.tzinfo:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None) + timedelta(hours=8)
        else:
            dt = dt + timedelta(hours=8)
        return dt
    except Exception:
        pass

    # ISO 8601 格式 (Atom): "2026-08-15T09:30:00Z" / "...+08:00" / 带毫秒
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S.%f%z"):
        try:
            dt = datetime.strptime(date_str, fmt)
            if dt.tzinfo:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None) + timedelta(hours=8)
            else:
                dt = dt + timedelta(hours=8)
            return dt
        except ValueError:
            continue

    return None


# ---------------------------------------------------------------------------
# 通用 RSS/Atom 采集
# ---------------------------------------------------------------------------
ATOM_NS = "{http://www.w3.org/2005/Atom}"


def parse_feed_entries(content, feed_name, need_filter, source_name,
                       source_type, start_date, end_date, max_items,
                       indirect=False):
    """解析 RSS/Atom 内容为 items 列表"""
    try:
        root = ET.fromstring(content)
    except ET.ParseError as e:
        raise RuntimeError(f"{feed_name} 解析失败: {e}")

    is_atom = root.tag.endswith("feed")
    entries = (root.findall(".//item") if not is_atom
               else root.findall(f".//{ATOM_NS}entry"))
    if not entries:
        entries = root.findall(".//entry")

    items = []
    for entry in entries:
        if is_atom:
            title = (entry.findtext(ATOM_NS + "title", default="") or "").strip()
        else:
            title = (entry.findtext("title") or "").strip()
        if not title:
            continue

        # AI 关键词过滤
        if need_filter:
            if not any(kw in title.lower() for kw in AI_KEYWORDS):
                continue

        # 链接
        link = ""
        if is_atom:
            link_elem = entry.find(ATOM_NS + "link")
            if link_elem is not None:
                link = (link_elem.get("href") or "").strip()
        else:
            link = (entry.findtext("link") or "").strip()

        # 摘要/描述(清洗 HTML 标签与纯链接, 避免出现"只甩一条链接"的摘要)
        summary = ""
        desc_elem = (entry.find(ATOM_NS + "summary") if is_atom
                     else entry.find("description"))
        if desc_elem is None and is_atom:
            desc_elem = entry.find(ATOM_NS + "content")
        if desc_elem is not None and desc_elem.text:
            summary = _clean_html(desc_elem.text)[:400]

        # 发布时间
        if is_atom:
            pub_raw = (entry.findtext(ATOM_NS + "published", default="")
                       or entry.findtext(ATOM_NS + "updated", default=""))
        else:
            pub_raw = entry.findtext("pubDate") or ""

        published = parse_rss_date(pub_raw)
        if published and not (start_date <= published < end_date):
            continue

        items.append({
            "title": title,
            "summary": summary,
            "url": link,
            "source": source_name,
            "source_type": source_type,
            "time": published or beijing_now(),
            "indirect": indirect,
        })
        if len(items) >= max_items:
            break
    return items


def collect_source(source, start_date, end_date):
    """采集单个 RSS/聚合源: 依次尝试候选 URL, 全部失败则降级 Google News。
    返回 (items, mode): mode = "direct" / "indirect" / 失败时抛异常"""
    name = source["name"]
    max_items = MAX_ITEMS_AGGREGATE if source["stype"] == "aggregate" else MAX_ITEMS_PER_SOURCE

    # 第一防线: 依次尝试候选 URL
    last_err = None
    for url_entry in source["urls"]:
        url = url_entry() if callable(url_entry) else url_entry
        resp = fetch_with_retry(url)
        if resp is None:
            last_err = f"所有候选URL不可用({url[:80]}...)"
            continue
        try:
            items = parse_feed_entries(resp.content, name, source["need_filter"],
                                       name, source["stype"], start_date, end_date,
                                       max_items, indirect=False)
            log(f"  {name} 直连采集 {len(items)} 条")
            return items, "direct"
        except RuntimeError as e:
            last_err = str(e)
            continue

    # 第二防线: Google News 间接获取
    if source.get("news_query"):
        gurl = google_news_url(source["news_query"], source.get("news_lang", "zh"))
        resp = fetch_with_retry(gurl)
        if resp is not None:
            try:
                items = parse_feed_entries(resp.content, name, False, name,
                                           source["stype"], start_date, end_date,
                                           max_items, indirect=True)
                log(f"  {name} 降级 Google News 间接获取 {len(items)} 条")
                return items, "indirect"
            except RuntimeError:
                pass

    raise RuntimeError(f"{name} 采集失败: {last_err or '无可用渠道'}")


# ---------------------------------------------------------------------------
# API 类采集器
# ---------------------------------------------------------------------------
def collect_arxiv(start_date, end_date):
    """arXiv API: AI/CL/LG 领域论文"""
    url = "https://export.arxiv.org/api/query"
    params = {
        "search_query": "cat:cs.AI OR cat:cs.CL OR cat:cs.LG",
        "sortBy": "submittedDate",
        "sortOrder": "descending",
        "max_results": str(MAX_ITEMS_PER_SOURCE * 3),
    }
    resp = fetch_with_retry(url, params=params)
    if resp is None:
        raise RuntimeError("arXiv API 不可用")

    ns = {"a": "http://www.w3.org/2005/Atom"}
    items = []
    root = ET.fromstring(resp.text)
    for entry in root.findall("a:entry", ns):
        published_raw = entry.findtext("a:published", default="", ns=ns)
        try:
            published = datetime.strptime(published_raw, "%Y-%m-%dT%H:%M:%SZ") + timedelta(hours=8)
        except ValueError:
            continue
        if not (start_date <= published < end_date):
            continue
        title = " ".join(entry.findtext("a:title", default="", ns=ns).split())
        summary = " ".join(entry.findtext("a:summary", default="", ns=ns).split())[:400]
        link = entry.findtext("a:id", default="", ns=ns)
        items.append({
            "title": title,
            "summary": summary,
            "url": link,
            "source": "arXiv",
            "source_type": "academic",
            "time": published,
            "indirect": False,
        })
        if len(items) >= MAX_ITEMS_PER_SOURCE:
            break
    log(f"  arXiv 采集 {len(items)} 条")
    return items


def collect_hackernews(start_date, end_date):
    """Hacker News API: 过滤 AI 关键词"""
    resp = fetch_with_retry("https://hacker-news.firebaseio.com/v0/topstories.json", timeout=10)
    if resp is None:
        raise RuntimeError("Hacker News API 不可用")
    story_ids = resp.json()[:HN_STORY_LIMIT]

    items = []
    for sid in story_ids:
        detail = fetch_with_retry(
            f"https://hacker-news.firebaseio.com/v0/item/{sid}.json",
            timeout=10, retries=1)
        if detail is None:
            continue
        story = detail.json()
        if not story or story.get("type") != "story":
            continue
        ts = story.get("time", 0)
        published = datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None) + timedelta(hours=8)
        if not (start_date <= published < end_date):
            continue
        title = story.get("title", "")
        if not any(kw in title.lower() for kw in AI_KEYWORDS):
            continue
        url = story.get("url") or f"https://news.ycombinator.com/item?id={sid}"
        items.append({
            "title": title,
            "summary": (story.get("text") or "")[:300],
            "url": url,
            "source": "Hacker News",
            "source_type": "community",
            "time": published,
            "indirect": False,
        })
        if len(items) >= MAX_ITEMS_PER_SOURCE:
            break
    log(f"  Hacker News 采集 {len(items)} 条")
    return items


def collect_github(start_date, end_date):
    """GitHub Search API: 前一天创建的热门仓库; 为空时放宽为'当日有更新且星数>50'"""
    headers = {"Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"

    queries = [
        {"q": f"created:{start_date.strftime('%Y-%m-%d')}..{end_date.strftime('%Y-%m-%d')}",
         "sort": "stars", "order": "desc"},
        {"q": f"stars:>100 pushed:>={start_date.strftime('%Y-%m-%d')} ai OR llm OR agent",
         "sort": "stars", "order": "desc"},
    ]
    for params in queries:
        params["per_page"] = str(MAX_ITEMS_PER_SOURCE)
        resp = fetch_with_retry("https://api.github.com/search/repositories",
                                headers=headers, params=params)
        if resp is None:
            continue
        data = resp.json()
        items = []
        for repo in data.get("items", []):
            created = repo.get("created_at", "")
            try:
                published = datetime.strptime(created[:10], "%Y-%m-%d")
            except ValueError:
                published = start_date
            items.append({
                "title": repo.get("full_name", ""),
                "summary": (repo.get("description") or "无描述")[:300],
                "url": repo.get("html_url", ""),
                "source": "GitHub",
                "source_type": "community",
                "time": published,
                "stars": repo.get("stargazers_count", 0),
                "indirect": False,
            })
            if len(items) >= MAX_ITEMS_PER_SOURCE:
                break
        if items:
            log(f"  GitHub 采集 {len(items)} 条")
            return items
    raise RuntimeError("GitHub API 不可用或无结果")


def collect_huggingface(start_date, end_date):
    """Hugging Face API: 近期热门模型(sort=likes7d 失败则降级 sort=likes)"""
    urls = [
        "https://huggingface.co/api/models?sort=likes7d&direction=-1&limit={}".format(
            MAX_ITEMS_PER_SOURCE * 2),
        "https://huggingface.co/api/models?sort=likes&direction=-1&limit={}".format(
            MAX_ITEMS_PER_SOURCE * 2),
    ]
    for url in urls:
        resp = fetch_with_retry(url)
        if resp is None:
            continue
        try:
            data = resp.json()
        except ValueError:
            continue
        items = []
        for model in data:
            model_id = model.get("id", "")
            if not model_id:
                continue
            created_raw = model.get("createdAt", "")
            try:
                published = datetime.strptime(created_raw[:10], "%Y-%m-%d")
            except (ValueError, TypeError):
                published = start_date
            if published < start_date or published >= end_date:
                continue
            likes = model.get("likes", 0)
            downloads = model.get("downloads", 0)
            summary = f"点赞 {likes}, 下载 {downloads}"
            tag_list = model.get("tags", [])
            if tag_list:
                summary += f", 标签: {', '.join(tag_list[:5])}"
            items.append({
                "title": f"🤗 {model_id}",
                "summary": summary[:300],
                "url": f"https://huggingface.co/{model_id}",
                "source": "Hugging Face",
                "source_type": "academic",
                "time": published,
                "indirect": False,
            })
            if len(items) >= MAX_ITEMS_PER_SOURCE:
                break
        if items:
            log(f"  Hugging Face 采集 {len(items)} 条")
            return items
    raise RuntimeError("Hugging Face API 不可用")


# ---------------------------------------------------------------------------
# 采集调度
# ---------------------------------------------------------------------------
def get_all_collectors():
    """返回统一采集器列表"""
    collectors = []
    for src in SOURCES:
        collectors.append({
            "name": src["name"],
            "kind": "source",
            "func": collect_source,
            "args": (src,),
        })
    collectors.append({"name": "arXiv",         "kind": "api", "func": collect_arxiv,        "args": ()})
    collectors.append({"name": "Hacker News",   "kind": "api", "func": collect_hackernews,  "args": ()})
    collectors.append({"name": "GitHub",        "kind": "api", "func": collect_github,       "args": ()})
    collectors.append({"name": "Hugging Face",  "kind": "api", "func": collect_huggingface, "args": ()})
    return collectors


def _run_collector(c, start_date, end_date):
    """执行单个采集器, 返回 items 列表(source类返回的元组已解包)"""
    if c["kind"] == "source":
        items, _mode = c["func"](*c["args"], start_date, end_date)
        return items
    return c["func"](start_date, end_date)


def collect_all(start_date, end_date):
    """采集所有源。
    返回 (items, indirect_map, failed_list)
      indirect_map: {源名: "Google News"} 表示该源经聚合间接获取
      failed_list : 完全失败的源名列表"""
    collectors = get_all_collectors()
    items = []
    indirect_map = {}
    failed = []

    for c in collectors:
        try:
            result = _run_collector(c, start_date, end_date)
            items.extend(result)
            # 判断该源是否为间接获取(通过条目标记)
            if result and all(it.get("indirect") for it in result):
                indirect_map[c["name"]] = "Google News 聚合"
        except Exception as e:
            failed.append(c["name"])
            log(f"  WARNING 源 {c['name']} 采集失败: {e}")

    # 末轮: 对失败源全量重试 1 次
    if failed:
        log(f"首轮完成, 失败源 {len(failed)} 个: {failed}, 等待 {FINAL_RETRY_DELAY}s 后末轮重试...")
        time.sleep(FINAL_RETRY_DELAY)
        recovered = []
        for name in list(failed):
            c = next((x for x in collectors if x["name"] == name), None)
            if not c:
                continue
            try:
                log(f"  末轮重试: {name}")
                result = _run_collector(c, start_date, end_date)
                items.extend(result)
                if result and all(it.get("indirect") for it in result):
                    indirect_map[name] = "Google News 聚合"
                recovered.append(name)
            except Exception as e:
                log(f"  末轮重试 {name} 仍然失败: {e}")
        for name in recovered:
            failed.remove(name)
        if failed:
            log(f"  最终失败源: {failed}")

    return items, indirect_map, failed


def retry_failed_sources(failed_names, start_date, end_date):
    """对指定失败源重新采集, 返回 (新增items, 仍失败列表, 新增间接获取map)"""
    collectors = {c["name"]: c for c in get_all_collectors()}
    new_items = []
    still_failed = []
    new_indirect = {}

    for name in failed_names:
        c = collectors.get(name)
        if not c:
            still_failed.append(name)
            continue
        try:
            log(f"  重采: {name}")
            result = _run_collector(c, start_date, end_date)
            new_items.extend(result)
            if result and all(it.get("indirect") for it in result):
                new_indirect[name] = "Google News 聚合"
            log(f"  重采成功: {name} ({len(result)} 条)")
        except Exception as e:
            log(f"  重采失败: {name}: {e}")
            still_failed.append(name)

    return new_items, still_failed, new_indirect


# ---------------------------------------------------------------------------
# LLM 摘要与分级(主模型 + 备用模型自动切换)
# ---------------------------------------------------------------------------
LLM_PROMPT_RULES = """处理规则:

【语言要求 — 最高优先级, 必须严格遵守】
1. 标题、摘要、关注理由全部必须用简体中文呈现, 无论原始报道是中文还是英文, 都必须翻译/改写为简体中文
2. 仅以下内容允许保留原文: 公司名(OpenAI/Anthropic/Google/ Meta/NVIDIA)、产品名(ChatGPT/Claude/Gemini)、
   模型名(GPT-5/Llama-4/GLM-5/DeepSeek-V4)、技术术语(Transformer/RAG/RLHF)、参数与数字(175B/700B/4万亿)
3. 严禁出现整句英文标题或整段英文摘要; 必要的英文术语须用中文语境包裹
   正确: "OpenAI 发布 GPT-5.6, Sol 模式输出速度达 750 tokens/秒"
   错误: "OpenAI announces GPT-5.6 with Sol mode" (整句英文, 禁止)

【合并与筛选】
4. 同一事件的多方报道合并为一条; 被合并来源用 " / " 连接放入 source_label (如 "36氪 / TechCrunch / The Verge")
5. 过滤低价值内容(纯营销软文、无实质转发、与AI无关)
6. 每个板块最多保留 20 条, 且必须挑选当天最值得关注的内容; 宁可少而精, 不要多而杂

【每条消息字段 — reason 为必填, 不可省略】
   - title: 简体中文标题, 简明扼要概括事件
   - summary: 2-4 句简体中文摘要, 提炼核心要点(关键数字、模型名、参数保留原文)
   - reason: 简体中文"关注理由", 1-2 句, 说明这条信息为什么值得关注(行业影响/技术突破/商业意义)
   - category: industry(行业新闻) / research(研究进展) / business(商业化动态) / github(GitHub与开源动态)
   - priority: high(重大事件) / medium(较重要) / normal(常规)
   - source_label: 来源名(简体中文优先, 国际媒体名保留原名, 多来源用 " / " 连接)
   - source_type: official / media / academic / community / unverified
     直接沿用输入条目 source_type; 若聚合索引但内容引自官方, 标 official 并在 source_label 注明
   - url: 信息最全的一条原文链接

【输出格式】
严格输出 JSON, 不要输出任何其他文字, 不要使用 markdown 代码块标记:
{"items": [{"title": "...", "summary": "...", "reason": "...", "category": "...",
"priority": "...", "source_label": "...", "source_type": "...", "url": "..."}]}"""


def _call_llm(base_url, api_key, model, prompt):
    """调用 LLM(OpenAI 兼容格式), 返回 items 列表或 None
    容错: key 缺失 / 401(密钥无效) / 400(参数不支持→去 response_format 重试) / 404(URL错误)"""
    if not api_key:
        log(f"  LLM({model}) 未配置 API Key, 无法调用(请在 GitHub Secrets 配置 LLM_API_KEY)")
        return None
    if not base_url:
        log(f"  LLM({model}) 未配置 Base URL")
        return None
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    def _post(body):
        resp = requests.post(f"{base_url}/chat/completions",
                             headers=headers, json=body, timeout=180)
        return resp

    body = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.3,
        "max_tokens": 8192,
        "response_format": {"type": "json_object"},
    }
    try:
        resp = _post(body)
    except requests.exceptions.Timeout:
        log(f"  LLM({model}) 请求超时(>180s), 请检查网络或换用备用模型")
        return None
    except Exception as e:
        log(f"  LLM({model}) 请求异常: {e}")
        return None

    # 部分 OpenAI 兼容服务不支持 response_format, 返回 400 时去掉它重试一次
    if resp.status_code == 400 and "response_format" in body:
        log(f"  LLM({model}) HTTP 400(可能不支持 response_format), 去掉后重试")
        body.pop("response_format", None)
        try:
            resp = _post(body)
        except Exception as e:
            log(f"  LLM({model}) 重试异常: {e}")
            return None

    if resp.status_code == 401:
        log(f"  LLM({model}) HTTP 401: API Key 无效或已过期, 请检查 LLM_API_KEY")
        return None
    if resp.status_code == 404:
        log(f"  LLM({model}) HTTP 404: Base URL 或模型名错误, 请检查 LLM_BASE_URL/LLM_MODEL")
        return None
    if resp.status_code != 200:
        log(f"  LLM({model}) HTTP {resp.status_code}: {resp.text[:300]}")
        return None

    try:
        content = resp.json()["choices"][0]["message"]["content"]
        data = json.loads(content)
        return data.get("items", [])
    except Exception as e:
        log(f"  LLM({model}) 返回解析异常: {e}")
        return None


def summarize_with_llm(items, is_supplement=False):
    """调用 LLM 做中文摘要/去重/分级, 自动主→备用切换, 返回结构化列表或 None"""
    payload_items = [
        {"title": it["title"], "summary": it["summary"][:200],
         "source": it["source"], "url": it["url"],
         "source_type": it.get("source_type", "unverified"),
         "indirect": bool(it.get("indirect"))}
        for it in items
    ]

    supplement_hint = ""
    if is_supplement:
        supplement_hint = ("\n注意: 这是补充更新, 以下为信息源恢复后重试获取的新增信息, "
                           "只保留新增内容。\n")

    prompt = (
        "你是资深AI资讯分析师, 为读者编写每日AI资讯日报。"
        "以下是过去24小时从多个信息源采集到的原始信息(JSON数组):\n"
        + json.dumps(payload_items, ensure_ascii=False)
        + "\n\n" + supplement_hint + LLM_PROMPT_RULES
    )

    log(f"  调用主 LLM: {LLM_MODEL} @ {LLM_BASE_URL} (输入 {len(payload_items)} 条)")
    result = _call_llm(LLM_BASE_URL, LLM_API_KEY, LLM_MODEL, prompt)
    if result is not None:
        log(f"  主 LLM 返回 {len(result)} 条")
        return result

    if LLM_BACKUP_API_KEY and LLM_BACKUP_BASE_URL and LLM_BACKUP_MODEL:
        log(f"  主模型失败, 切换备用 LLM: {LLM_BACKUP_MODEL} @ {LLM_BACKUP_BASE_URL}")
        result = _call_llm(LLM_BACKUP_BASE_URL, LLM_BACKUP_API_KEY, LLM_BACKUP_MODEL, prompt)
        if result is not None:
            log(f"  备用 LLM 返回 {len(result)} 条")
            return result

    return None


def fallback_format(items):
    """LLM 全部失败时的降级: 规则整理 + 清洗(去HTML/去纯链接/空摘要兜底)
    注意: 无 LLM 无法翻译, 标题/摘要保持原始语言, 需在日报页脚明确告知读者"""
    result = []
    for it in items:
        source_type = it.get("source_type", "unverified")
        if it["source"] in ("GitHub", "Hacker News"):
            category = "github"
        elif it["source"] in ("arXiv", "Hugging Face"):
            category = "research"
        else:
            category = "industry"
        # 清洗标题与摘要(去HTML标签/纯链接/实体)
        title = _clean_html(it["title"]) or "（无标题）"
        summary = _clean_html(it["summary"])
        if not summary:
            summary = "原文未提供摘要，请点击来源链接查看详情。"
        result.append({
            "title": title,
            "summary": summary[:200],
            "reason": "AI 智能整理服务暂不可用，本条为原始采集内容（未经翻译与分级），请参阅摘要与来源链接自行判断。",
            "category": category,
            "priority": "normal",
            "source_label": it["source"] + ("（间接获取）" if it.get("indirect") else ""),
            "source_type": source_type,
            "url": it["url"],
        })
    return result


# ---------------------------------------------------------------------------
# 日报格式化(标准 Markdown 文档)
# ---------------------------------------------------------------------------
CATEGORY_MAP = [
    ("industry", "## 📊 行业新闻"),
    ("research", "## 🔬 研究进展"),
    ("business", "## 💰 商业化动态"),
    ("github",   "## 💻 GitHub动态"),
]
SOURCE_TYPE_MAP = {
    "official": "官方确认",
    "media": "权威媒体报道",
    "academic": "学术平台",
    "community": "社区动态",
    "unverified": "待核实",
}


def _fmt_item(idx, it):
    """格式化单条消息: 加粗标题(高优先级加🔴) + 摘要 + 关注理由 + 来源超链接"""
    priority = it.get("priority", "normal")
    marker = "🔴 " if priority == "high" else ""
    lines = [f"{idx}. {marker}**{it['title']}**"]
    lines.append(f"   - 摘要：{it['summary']}")
    reason = it.get("reason")
    if reason:
        lines.append(f"   - 关注理由：{reason}")
    label = it.get("source_label") or it.get("source", "未知来源")
    url = it.get("url", "")
    rating = SOURCE_TYPE_MAP.get(it.get("source_type"), "待核实")
    if url:
        lines.append(f"   - 来源：[{label}]({url}) 【{rating}】")
    else:
        lines.append(f"   - 来源：{label} 【{rating}】")
    return "\n".join(lines)


def _archive_url(iso_label):
    """构造今日日报在 GitHub 仓库的归档链接(在 Actions 环境中可用)"""
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    if not repo:
        return ""
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    branch = os.environ.get("GITHUB_REF_NAME", "") or "main"
    filename = quote(f"AI资讯日报_{iso_label}.md")
    return f"{server}/{repo}/raw/{branch}/reports/{filename}"


def build_report_md(items, indirect_map, failed, cover_label_cn, iso_label,
                    is_supplement=False):
    """生成标准 Markdown 日报文档(四大板块, 每板块≤20条, 每条带关注理由)"""
    title = ("📰 AI资讯日报补充更新" if is_supplement else "📰 AI资讯日报") + f" | {cover_label_cn}"
    lines = [f"# {title}", ""]
    lines.append(f"> 数据覆盖：{cover_label_cn} 00:00-24:00 ｜ 信息源：{len(SOURCES) + 4} 个"
                 f"（官方博客 / 权威媒体 / 学术平台 / 社区）")
    lines.append("")

    # 四大板块: 每板块按优先级排序, 截断至 MAX_PER_CATEGORY 条
    priority_order = {"high": 0, "medium": 1, "normal": 2}
    for cat_key, cat_title in CATEGORY_MAP:
        cat_items = [it for it in items if it.get("category") == cat_key]
        cat_items.sort(key=lambda x: priority_order.get(x.get("priority", "normal"), 2))
        cat_items = cat_items[:MAX_PER_CATEGORY]
        lines.append("---")
        lines.append("")
        lines.append(cat_title)
        lines.append("")
        if not cat_items:
            lines.append("本板块今日无重大动态")
            lines.append("")
            continue
        for i, it in enumerate(cat_items, 1):
            lines.append(_fmt_item(i, it))
        lines.append("")

    # 信息采集异常
    if indirect_map or failed:
        lines.append("---")
        lines.append("")
        lines.append("## ⚠️ 信息采集异常")
        lines.append("")
        if indirect_map:
            lines.append("以下信息源因网络、反爬或URL变更原因未能直接获取原始页面，"
                         "相关内容已通过聚合源间接获取，原始来源已随文标注：")
            lines.append("")
            for name, via in indirect_map.items():
                lines.append(f"- **{name}**：经{via}间接获取")
            lines.append("")
        if failed:
            lines.append("以下信息源完全不可用，将于30分钟后自动重试，成功后将推送补充更新：")
            lines.append("")
            for name in failed:
                lines.append(f"- **{name}**：网络/服务原因暂未能获取")
            lines.append("")

    # 页脚
    lines.append("---")
    lines.append("")
    archive = _archive_url(iso_label)
    if archive:
        lines.append(f"📄 完整 Markdown 归档：[{archive}]({archive})")
        lines.append("")
    lines.append("🤖 由 GitHub Actions 自动生成并发送")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 飞书推送(交互卡片 + Markdown 渲染, 支持加签)
# ---------------------------------------------------------------------------
def _feishu_sign_params():
    """生成飞书机器人加签参数"""
    if not FEISHU_SECRET:
        return None
    timestamp = str(int(time.time()))
    string_to_sign = f"{timestamp}\n{FEISHU_SECRET}"
    hmac_code = hmac.new(string_to_sign.encode("utf-8"), digestmod=hashlib.sha256).digest()
    sign = base64.b64encode(hmac_code).decode("utf-8")
    return {"timestamp": timestamp, "sign": sign}


def _post_feishu(payload):
    """发送一次飞书请求, 返回 (成功?, 响应)"""
    for attempt in range(3):
        try:
            resp = requests.post(FEISHU_WEBHOOK, params=_feishu_sign_params(),
                                 json=payload, timeout=15)
            data = resp.json()
            if data.get("code") == 0:
                return True, data
            log(f"  飞书返回异常(第{attempt + 1}次): {data}")
        except Exception as e:
            log(f"  飞书请求异常(第{attempt + 1}次): {e}")
        time.sleep(3)
    return False, None


def chunk_markdown(md_text, limit=CARD_CHUNK_CHARS):
    """把 Markdown 文档按行边界切片, 每片不超过 limit 字符"""
    chunks = []
    current = []
    current_len = 0
    for line in md_text.split("\n"):
        line_len = len(line) + 1
        if current_len + line_len > limit and current:
            chunks.append("\n".join(current))
            current, current_len = [], 0
        current.append(line)
        current_len += line_len
    if current:
        chunks.append("\n".join(current))
    return chunks


def send_md_to_feishu(md_text, card_title):
    """以单张交互卡片推送完整 Markdown 文档(不再分片为多条消息);
    内容超限则截断并提示见仓库归档; 卡片失败时降级纯文本"""
    log(f"  推送单张卡片(共 {len(md_text)} 字符)")
    content = md_text
    if len(content) > CARD_CONTENT_LIMIT:
        content = content[:CARD_CONTENT_LIMIT] + \
            "\n\n...(内容过长已截断, 完整内容见仓库 reports/ 目录归档)"
        log(f"  内容超限, 截断至 {CARD_CONTENT_LIMIT} 字符")
    elements = [{"tag": "markdown", "content": content}]
    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": card_title[:48]},
            "template": "blue",
        },
        "elements": elements,
    }
    ok, _ = _post_feishu({"msg_type": "interactive", "card": card})
    if ok:
        return True
    # 降级: 纯文本(损失排版但保证送达)
    log("  单卡片推送失败, 降级为纯文本")
    text = md_text[:28000]
    if len(md_text) > 28000:
        text += "\n\n...(内容过长, 已截断)"
    ok, _ = _post_feishu({"msg_type": "text", "content": {"text": text}})
    return ok


def save_report_file(md_text, iso_label, is_supplement=False):
    """把日报归档为本地 Markdown 文件(workflow 会提交到仓库 reports/ 目录)"""
    if not REPORT_ARCHIVE:
        return None
    os.makedirs("reports", exist_ok=True)
    suffix = "-补充更新" if is_supplement else ""
    path = os.path.join("reports", f"AI资讯日报_{iso_label}{suffix}.md")
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(md_text)
        log(f"  日报已归档: {path}")
        return path
    except Exception as e:
        log(f"  日报归档失败: {e}")
        return None


def commit_reports_to_repo(iso_label):
    """把 reports/ 目录 commit 并 push 到 GitHub 仓库, 使归档链接立即生效。
    修复点: GitHub Actions 的 checkout 处于 detached HEAD, 裸 `git push` 会报
    "You are not currently on a branch", 必须用 `git push origin HEAD:<branch>`。
    仅在 GitHub Actions 环境(GITHUB_TOKEN 存在)执行; 失败不阻塞飞书推送。"""
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    repo = os.environ.get("GITHUB_REPOSITORY", "").strip()
    if not (token and repo):
        log("  非 GitHub Actions 环境(无 GITHUB_TOKEN), 跳过自动归档")
        return

    branch = os.environ.get("GITHUB_REF_NAME", "").strip() or "main"

    def _run(cmd):
        try:
            return subprocess.run(cmd, capture_output=True, text=True)
        except FileNotFoundError:
            log("  未找到 git 命令, 跳过自动归档")
            return None

    _run(["git", "config", "user.name", "github-actions[bot]"])
    _run(["git", "config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com"])

    r = _run(["git", "add", "reports/"])
    if r is None:
        return
    r = _run(["git", "diff", "--cached", "--quiet"])
    if r is not None and r.returncode == 0:
        log("  无新归档文件, 跳过 commit/push")
        return

    _run(["git", "commit", "-m", f"docs: 归档 AI 资讯日报 {iso_label}"])
    r = _run(["git", "push", "origin", f"HEAD:{branch}"])
    if r is not None and r.returncode == 0:
        log(f"  日报已归档并 push 到 {branch}")
    else:
        err = (r.stderr if r else "")[:300]
        log(f"  push 失败(归档链接可能暂时 404): {err}")


# ---------------------------------------------------------------------------
# 主函数
# ---------------------------------------------------------------------------
def main():
    log("===== 每日AI资讯日报开始 =====")

    if not FEISHU_WEBHOOK:
        log("错误: 未配置 FEISHU_WEBHOOK")
        return {"code": 1, "msg": "FEISHU_WEBHOOK 未配置"}
    if not LLM_API_KEY:
        log("错误: 未配置 LLM_API_KEY, 将无法调用 LLM 生成中文日报(可用规则降级)")
        # 不直接退出: 仍采集并尝试规则降级, 但会提示

    # LLM 配置自检(脱敏)
    log(f"LLM 主模型: {LLM_MODEL} @ {LLM_BASE_URL} (key: {_mask_key(LLM_API_KEY)})")
    if LLM_BACKUP_API_KEY:
        log(f"LLM 备用模型: {LLM_BACKUP_MODEL} @ {LLM_BACKUP_BASE_URL} (key: {_mask_key(LLM_BACKUP_API_KEY)})")
    else:
        log("未配置备用 LLM, 主模型失败时将使用规则降级")

    # 时间窗口: 前一天 00:00:00 ~ 当天 00:00:00 (北京时间)
    now = beijing_now()
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = today - timedelta(days=1)
    end_date = today
    iso_label = start_date.strftime("%Y-%m-%d")
    cover_label_cn = f"{start_date.year}年{start_date.month}月{start_date.day}日"
    log(f"数据窗口: {iso_label} 00:00-24:00")

    # ===== 第一阶段: 全量采集 + 推送日报 =====
    items, indirect_map, failed = collect_all(start_date, end_date)
    log(f"首轮采集完成: 共 {len(items)} 条, 间接获取 {len(indirect_map)} 源, "
        f"失败 {len(failed)} 源")

    if items:
        structured = summarize_with_llm(items)
        if structured is None:
            log("LLM 全部失败(主+备用), 使用规则降级(注意: 降级输出为原始采集, 可能含英文)")
            structured = fallback_format(items)
    else:
        structured = []

    report = build_report_md(structured, indirect_map, failed,
                             cover_label_cn, iso_label)
    save_report_file(report, iso_label)
    # 先归档 commit+push 到仓库, 再推送卡片, 保证归档链接立即有效
    commit_reports_to_repo(iso_label)
    card_title = f"📰 AI资讯日报 | {cover_label_cn}"
    ok = send_md_to_feishu(report, card_title)
    log("首轮日报推送" + ("成功" if ok else "失败"))

    # ===== 第二阶段: 真失败源 30 分钟后重采 =====
    if not failed:
        log("无失败源, 任务完成")
        log("===== 每日AI资讯日报执行成功 =====")
        return {"code": 0, "msg": "success", "items": len(items), "failed": []}

    log(f"有 {len(failed)} 个源完全失败: {failed}, 等待 30 分钟后重采...")
    time.sleep(RETRY_WAIT_SECONDS)

    supplement_items, still_failed, supp_indirect = retry_failed_sources(
        failed, start_date, end_date)
    log(f"重采完成: 新增 {len(supplement_items)} 条, 仍失败: {still_failed or '无'}")

    if supplement_items:
        structured = summarize_with_llm(supplement_items, is_supplement=True)
        if structured is None:
            structured = fallback_format(supplement_items)
        supp_indirect_full = dict(indirect_map)
        supp_indirect_full.update(supp_indirect)
        supp_report = build_report_md(structured, supp_indirect_full, still_failed,
                                      cover_label_cn, iso_label, is_supplement=True)
        save_report_file(supp_report, iso_label, is_supplement=True)
        commit_reports_to_repo(iso_label)
        ok2 = send_md_to_feishu(supp_report, f"📰 AI资讯日报补充更新 | {cover_label_cn}")
        log("补充更新推送" + ("成功" if ok2 else "失败"))

    if still_failed:
        final_lines = [
            f"⚠️ **AI资讯日报最终采集状态** | {cover_label_cn}", "",
            "以下信息源经30分钟重试后仍不可用：", "",
        ]
        final_lines += [f"- **{name}**" for name in still_failed]
        final_lines += ["", "不再进行第三次重试。如需查看，请访问对应网站。"]
        send_md_to_feishu("\n".join(final_lines), "⚠️ 采集异常通知")
        log(f"已推送最终失败通知: {still_failed}")

    log("===== 每日AI资讯日报执行成功 =====")
    return {
        "code": 0, "msg": "success", "items": len(items),
        "failed": failed, "still_failed": still_failed,
        "supplement": len(supplement_items) > 0,
    }


if __name__ == "__main__":
    main()
