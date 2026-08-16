# -*- coding: utf-8 -*-
"""
每日 AI 资讯日报 - GitHub Actions 版
=====================================
GitHub Actions 定时触发, 每天 09:00(北京时间)执行:
  1. 采集过去 24 小时 AI 领域资讯(18 个信息源, 覆盖国内外)
  2. 调用 LLM API 做摘要、去重、重要性分级(主模型+备用模型自动切换)
  3. 格式化为结构化日报, 通过飞书群机器人 Webhook 推送
  4. 如有采集失败的源, 等待 30 分钟后重采失败源, 推送"补充更新"

架构:
  GitHub Actions cron(UTC 01:00 = 北京 09:00) → ubuntu-latest runner
  → Python 脚本执行 → 采集 → LLM 摘要 → 飞书推送
  整个过程在 GitHub 云端服务器上运行, 与你的电脑完全无关。

信息源分层(18 个):
  - 官方博客(official): OpenAI / Anthropic / Google AI / Google DeepMind /
    Meta AI / Microsoft AI / Mistral AI
  - 权威媒体(media): TechCrunch / VentureBeat / The Verge / MIT Tech Review /
    机器之心 / 量子位 / 36氪
  - 学术平台(academic): arXiv / Hugging Face
  - 社区动态(community): Hacker News / GitHub

备用机制:
  - 每源多轮重试(首轮3次间隔5秒 + 末轮全量重试1次间隔10秒)
  - LLM 主模型失败 → 自动切换备用模型 → 仍失败则规则降级
  - 30分钟后内部重采失败源, 推送补充更新
  - 飞书推送失败重试3次

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
"""

import base64
import hashlib
import hmac
import json
import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import requests

# ---------------------------------------------------------------------------
# 配置(从环境变量读取)
# ---------------------------------------------------------------------------
FEISHU_WEBHOOK = os.environ.get("FEISHU_WEBHOOK", "").strip()
FEISHU_SECRET = os.environ.get("FEISHU_SECRET", "").strip()
LLM_API_KEY = os.environ.get("LLM_API_KEY", "").strip()
LLM_BASE_URL = os.environ.get("LLM_BASE_URL", "https://api.deepseek.com/v1").strip().rstrip("/")
LLM_MODEL = os.environ.get("LLM_MODEL", "deepseek-chat").strip()
LLM_BACKUP_API_KEY = os.environ.get("LLM_BACKUP_API_KEY", "").strip()
LLM_BACKUP_BASE_URL = os.environ.get("LLM_BACKUP_BASE_URL", "").strip().rstrip("/")
LLM_BACKUP_MODEL = os.environ.get("LLM_BACKUP_MODEL", "").strip()
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()

# 采集参数
MAX_ITEMS_PER_SOURCE = 10         # 每个源最多采集条数
REQUEST_TIMEOUT = 15              # 网络请求超时(秒)
RETRY_TIMES = 3                   # 每个源首轮重试次数
RETRY_DELAY = 5                   # 首轮重试间隔(秒)
FINAL_RETRY_DELAY = 10            # 末轮全量重试间隔(秒)
HN_STORY_LIMIT = 80               # Hacker News 拉取条目数
RETRY_WAIT_SECONDS = 1800         # 失败源重采等待时间(30分钟)

# AI 关键词(用于综合媒体过滤)
AI_KEYWORDS = [
    "ai", "artificial intelligence", "llm", "gpt", "chatgpt", "openai",
    "anthropic", "claude", "deepseek", "gemini", "bard", "midjourney",
    "stable diffusion", "diffusion", "transformer", "neural", "machine learning",
    "deep learning", "agent", "copilot", "mistral", "llama", "qwen", "glm",
    "sora", "runway", "perplexity", "grok", "xai", "deepmind",
    "大模型", "人工智能", "智能体", "机器学习", "深度学习", "算力",
    "多模态", "自动驾驶", "具身智能", "AGI",
]

# 通用请求头(避免被部分网站拦截)
COMMON_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "application/rss+xml, application/xml, application/json, text/xml, */*",
}

# ---------------------------------------------------------------------------
# RSS 源配置
# 格式: (源名称, RSS/Atom URL, 是否需要AI关键词过滤, 源类型)
#   源类型: official(官方博客) / media(权威媒体) / academic(学术) / community(社区)
# ---------------------------------------------------------------------------
RSS_FEEDS = [
    # === 国际官方博客 (权威性最高) ===
    ("OpenAI Blog",          "https://openai.com/blog/rss.xml",                          False, "official"),
    ("Anthropic Blog",       "https://www.anthropic.com/news/rss.xml",                   False, "official"),
    ("Google AI Blog",       "http://googleaiblog.blogspot.com/feeds/posts/default",     False, "official"),
    ("Google DeepMind Blog", "https://deepmind.google/blog/rss.xml",                     False, "official"),
    ("Meta AI Blog",         "https://ai.meta.com/blog/rss/",                            False, "official"),
    ("Microsoft AI Blog",    "https://blogs.microsoft.com/ai/feed/",                     False, "official"),
    ("Mistral AI Blog",      "https://mistral.ai/news/rss.xml",                          False, "official"),

    # === 国际权威科技媒体 ===
    ("TechCrunch AI",        "https://techcrunch.com/category/artificial-intelligence/feed/", False, "media"),
    ("VentureBeat AI",       "https://venturebeat.com/category/ai/feed/",                False, "media"),
    ("The Verge AI",         "https://www.theverge.com/rss/ai-artificial-intelligence/index.xml", False, "media"),
    ("MIT Tech Review AI",   "https://www.technologyreview.com/topic/artificial-intelligence/feed/", False, "media"),

    # === 国内权威媒体 ===
    ("机器之心",             "https://www.jiqizhixin.com/rss",                           False, "media"),
    ("量子位",               "https://www.qbitai.com/feed",                              False, "media"),
    ("36氪",                 "https://36kr.com/feed",                                    True,  "media"),
]


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------
def beijing_now():
    """返回北京时间(UTC+8)当前时间(无时区信息)"""
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=8)


def log(msg):
    print(f"[{beijing_now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def fetch_with_retry(url, headers=None, params=None, timeout=REQUEST_TIMEOUT,
                     retries=RETRY_TIMES, delay=RETRY_DELAY):
    """带重试的 GET 请求, 成功返回 requests.Response, 失败返回 None"""
    final_headers = dict(COMMON_HEADERS)
    if headers:
        final_headers.update(headers)
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, headers=final_headers, params=params, timeout=timeout)
            if resp.status_code == 200:
                return resp
            log(f"  HTTP {resp.status_code}: {url}")
        except Exception as e:
            log(f"  请求异常({attempt + 1}/{retries + 1}): {e}")
        if attempt < retries:
            time.sleep(delay)
    return None


def parse_rss_date(date_str):
    """解析 RSS/Atom 日期字符串, 返回 naive datetime(已转北京时间 UTC+8)"""
    if not date_str:
        return None
    date_str = date_str.strip()

    # 尝试 RFC 822 格式 (RSS 2.0): "Wed, 15 Aug 2026 09:30:00 +0000"
    try:
        dt = parsedate_to_datetime(date_str)
        if dt.tzinfo:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None) + timedelta(hours=8)
        else:
            dt = dt + timedelta(hours=8)
        return dt
    except Exception:
        pass

    # 尝试 ISO 8601 格式 (Atom): "2026-08-15T09:30:00Z" 或 "2026-08-15T09:30:00+08:00"
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
# 通用 RSS/Atom 采集器
# ---------------------------------------------------------------------------
def collect_rss_feed(feed_name, feed_url, need_filter, source_type, start_date, end_date):
    """通用 RSS/Atom 采集器, 返回 items 列表"""
    resp = fetch_with_retry(feed_url)
    if resp is None:
        raise RuntimeError(f"{feed_name} RSS 不可用(网络或服务故障)")

    items = []
    try:
        root = ET.fromstring(resp.content)
    except ET.ParseError as e:
        raise RuntimeError(f"{feed_name} RSS 解析失败: {e}")

    # 检测格式: RSS 2.0 (<rss><channel><item>) 或 Atom (<feed><entry>)
    is_atom = root.tag.endswith("feed")

    entries = root.findall(".//item") if not is_atom else root.findall(".//{http://www.w3.org/2005/Atom}entry")
    if not entries:
        # 尝试不带命名空间的 entry
        entries = root.findall(".//entry")

    for entry in entries:
        # 提取标题
        title = ""
        if not is_atom:
            title = (entry.findtext("title") or "").strip()
        else:
            title = (entry.findtext("{http://www.w3.org/2005/Atom}title", default="") or "").strip()
            if not title:
                title = (entry.findtext("title", default="") or "").strip()

        # AI 关键词过滤
        if need_filter:
            if not any(kw in title.lower() for kw in AI_KEYWORDS):
                continue

        # 提取链接
        link = ""
        if not is_atom:
            link = (entry.findtext("link") or "").strip()
        else:
            link_elem = entry.find("{http://www.w3.org/2005/Atom}link")
            if link_elem is not None:
                link = link_elem.get("href", "").strip()
            if not link:
                link_elem = entry.find("link")
                if link_elem is not None:
                    link = link_elem.get("href", "").strip()

        # 提取摘要/描述
        summary = ""
        if not is_atom:
            desc_elem = entry.find("description")
        else:
            desc_elem = entry.find("{http://www.w3.org/2005/Atom}summary")
            if desc_elem is None:
                desc_elem = entry.find("{http://www.w3.org/2005/Atom}content")
        if desc_elem is not None and desc_elem.text:
            summary = " ".join(desc_elem.text.split())[:400]

        # 提取发布时间
        if not is_atom:
            pub_raw = entry.findtext("pubDate") or ""
        else:
            pub_raw = (entry.findtext("{http://www.w3.org/2005/Atom}published", default="")
                       or entry.findtext("{http://www.w3.org/2005/Atom}updated", default="")
                       or entry.findtext("published", default="")
                       or entry.findtext("updated", default=""))

        published = parse_rss_date(pub_raw)
        if published and not (start_date <= published < end_date):
            continue

        items.append({
            "title": title,
            "summary": summary,
            "url": link,
            "source": feed_name,
            "source_type": source_type,
            "time": published or beijing_now(),
        })
        if len(items) >= MAX_ITEMS_PER_SOURCE:
            break

    log(f"  {feed_name} 采集 {len(items)} 条")
    return items


# ---------------------------------------------------------------------------
# 特殊采集器(API 类)
# ---------------------------------------------------------------------------
def collect_arxiv(start_date, end_date):
    """arXiv API: AI/CL/LG 领域论文"""
    url = "http://export.arxiv.org/api/query"
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
        detail = fetch_with_retry(f"https://hacker-news.firebaseio.com/v0/item/{sid}.json",
                                  timeout=10, retries=1, delay=2)
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
        })
        if len(items) >= MAX_ITEMS_PER_SOURCE:
            break
    log(f"  Hacker News 采集 {len(items)} 条")
    return items


def collect_github(start_date, end_date):
    """GitHub Search API: 前一天创建的热门仓库"""
    day_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    query = f"created:{day_str}..{end_str}"
    headers = {"Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"
    params = {"q": query, "sort": "stars", "order": "desc", "per_page": str(MAX_ITEMS_PER_SOURCE)}
    resp = fetch_with_retry("https://api.github.com/search/repositories", headers=headers, params=params)
    if resp is None:
        raise RuntimeError("GitHub API 不可用")
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
        })
    log(f"  GitHub 采集 {len(items)} 条")
    return items


def collect_huggingface(start_date, end_date):
    """Hugging Face API: 近期热门模型"""
    url = "https://huggingface.co/api/models"
    params = {"sort": "likes7d", "direction": "-1", "limit": str(MAX_ITEMS_PER_SOURCE * 2)}
    headers = {}
    resp = fetch_with_retry(url, headers=headers, params=params, timeout=15)
    if resp is None:
        raise RuntimeError("Hugging Face API 不可用")
    data = resp.json()
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
        })
        if len(items) >= MAX_ITEMS_PER_SOURCE:
            break
    log(f"  Hugging Face 采集 {len(items)} 条")
    return items


# ---------------------------------------------------------------------------
# 采集调度
# ---------------------------------------------------------------------------
def get_all_collectors():
    """返回所有采集器列表: (源名称, 可调用对象, 是否RSS, RSS配置元组)"""
    collectors = []
    # RSS 源
    for name, url, need_filter, stype in RSS_FEEDS:
        collectors.append({
            "name": name,
            "type": "rss",
            "func": collect_rss_feed,
            "args": (name, url, need_filter, stype),
        })
    # API 源
    collectors.append({"name": "arXiv",         "type": "api", "func": collect_arxiv,        "args": ()})
    collectors.append({"name": "Hacker News",   "type": "api", "func": collect_hackernews,    "args": ()})
    collectors.append({"name": "GitHub",        "type": "api", "func": collect_github,         "args": ()})
    collectors.append({"name": "Hugging Face",  "type": "api", "func": collect_huggingface,    "args": ()})
    return collectors


def collect_all(start_date, end_date):
    """采集所有源, 返回 (items, failed_sources)
    每个源首轮重试 RETRY_TIMES 次; 全部完成后对失败源做末轮重试 1 次"""
    collectors = get_all_collectors()
    items = []
    failed = []

    # 第一轮: 逐个采集
    for c in collectors:
        try:
            if c["type"] == "rss":
                result = c["func"](*c["args"], start_date, end_date)
            else:
                result = c["func"](start_date, end_date)
            items.extend(result)
        except Exception as e:
            failed.append(c["name"])
            log(f"  WARNING 源 {c['name']} 采集失败: {e}")

    # 第二轮(末轮): 对失败源全量重试 1 次
    if failed:
        log(f"第一轮采集完成, 失败源 {len(failed)} 个: {failed}, 等待 {FINAL_RETRY_DELAY}s 后末轮重试...")
        time.sleep(FINAL_RETRY_DELAY)
        retry_success = []
        for name in list(failed):
            c = next((x for x in collectors if x["name"] == name), None)
            if not c:
                continue
            try:
                log(f"  末轮重试: {name}")
                if c["type"] == "rss":
                    result = c["func"](*c["args"], start_date, end_date)
                else:
                    result = c["func"](start_date, end_date)
                items.extend(result)
                retry_success.append(name)
            except Exception as e:
                log(f"  末轮重试 {name} 仍然失败: {e}")
        for name in retry_success:
            failed.remove(name)
        if retry_success:
            log(f"  末轮重试成功 {len(retry_success)} 个: {retry_success}")
        if failed:
            log(f"  最终失败源: {failed}")

    return items, failed


# ---------------------------------------------------------------------------
# LLM 摘要与分级(支持主模型 + 备用模型自动切换)
# ---------------------------------------------------------------------------
def _call_llm(base_url, api_key, model, prompt):
    """调用 LLM(OpenAI 兼容格式), 返回 items 列表或 None"""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    body = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.3,
        "max_tokens": 6000,
        "response_format": {"type": "json_object"},
    }
    try:
        resp = requests.post(
            f"{base_url}/chat/completions",
            headers=headers,
            json=body,
            timeout=120,
        )
        if resp.status_code != 200:
            log(f"  LLM({model}) HTTP {resp.status_code}: {resp.text[:300]}")
            return None
        content = resp.json()["choices"][0]["message"]["content"]
        data = json.loads(content)
        return data.get("items", [])
    except Exception as e:
        log(f"  LLM({model}) 调用异常: {e}")
        return None


def summarize_with_llm(items, is_supplement=False):
    """调用 LLM 进行去重/过滤/摘要/分级, 自动主→备用切换, 返回结构化列表或 None"""
    payload_items = [
        {"title": it["title"], "summary": it["summary"][:200],
         "source": it["source"], "url": it["url"],
         "source_type": it.get("source_type", "unverified")}
        for it in items
    ]

    supplement_hint = ""
    if is_supplement:
        supplement_hint = (
            "\n注意: 这是补充更新, 部分信息源在首次采集时失败, 以下为30分钟后重试获取的新增信息。"
            "请只保留新增内容, 标题用'补充更新'。"
        )

    prompt = (
        "你是资深AI资讯分析师。以下是过去24小时采集到的AI领域原始信息(JSON数组):\n"
        + json.dumps(payload_items, ensure_ascii=False)
        + f"\n\n请完成以下处理:{supplement_hint}\n"
        "1. 合并同一事件的重复报道, 只保留信息最全的一条\n"
        "2. 过滤低价值信息(纯营销、无实质内容的转发)\n"
        "3. 为每条保留的消息写2-3句中文摘要, 提炼核心要点\n"
        "4. 按重要程度分级: high(高, 重大事件)/medium(中)/normal(常规)\n"
        "5. 归类到板块: industry(行业新闻)/research(研究进展)/business(商业化)/github(GitHub动态)\n"
        "6. 判定信息来源类型: official(官方公告)/media(权威媒体)/community(社区)/unverified(待核实)\n"
        "7. 对 high 级别的消息, 附上简短关注理由(reason字段)\n"
        "\n只输出JSON, 格式: {\"items\": [{\"title\": \"...\", \"summary\": \"...\", "
        "\"category\": \"...\", \"priority\": \"...\", \"source\": \"...\", "
        "\"source_type\": \"...\", \"url\": \"...\", \"reason\": \"...(仅high级别)\"}]}\n"
        "不要输出任何其他文字。"
    )

    # 主模型
    log(f"  调用主 LLM: {LLM_MODEL} @ {LLM_BASE_URL}")
    result = _call_llm(LLM_BASE_URL, LLM_API_KEY, LLM_MODEL, prompt)
    if result is not None:
        log(f"  主 LLM 返回 {len(result)} 条")
        return result

    # 备用模型
    if LLM_BACKUP_API_KEY and LLM_BACKUP_BASE_URL and LLM_BACKUP_MODEL:
        log(f"  主模型失败, 切换备用 LLM: {LLM_BACKUP_MODEL} @ {LLM_BACKUP_BASE_URL}")
        result = _call_llm(LLM_BACKUP_BASE_URL, LLM_BACKUP_API_KEY, LLM_BACKUP_MODEL, prompt)
        if result is not None:
            log(f"  备用 LLM 返回 {len(result)} 条")
            return result

    return None


def fallback_format(items):
    """LLM 全部失败时的降级: 用规则简单整理(不智能摘要、不智能分级)"""
    result = []
    for it in items:
        source_type = it.get("source_type", "unverified")
        category = "github" if it["source"] == "GitHub" else "industry"
        if it["source"] == "arXiv" or it["source"] == "Hugging Face":
            category = "research"
        result.append({
            "title": it["title"],
            "summary": it["summary"][:150] if it["summary"] else "无摘要",
            "category": category,
            "priority": "normal",
            "source": it["source"],
            "source_type": source_type,
            "url": it["url"],
        })
    return result


# ---------------------------------------------------------------------------
# 日报格式化
# ---------------------------------------------------------------------------
CATEGORY_MAP = {
    "industry": "📊 行业新闻",
    "research": "🔬 研究进展",
    "business": "💰 商业化动态",
    "github": "💻 GitHub动态",
}
PRIORITY_MAP = {
    "high": "🔴 高优先级",
    "medium": "🟡 中优先级",
    "normal": "⚪ 常规",
}
SOURCE_TYPE_MAP = {
    "official": "官方确认",
    "media": "权威媒体报道",
    "academic": "学术平台",
    "community": "社区动态",
    "unverified": "待核实",
}


def build_report(items, failed, cover_label, is_supplement=False):
    """生成日报文本"""
    lines = []
    title_prefix = "📰 AI资讯日报补充更新" if is_supplement else "📰 AI资讯日报"
    lines.append(f"{title_prefix} | {cover_label}")
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━")

    # 高优先级关注
    high_items = [it for it in items if it.get("priority") == "high"]
    if high_items:
        lines.append("")
        lines.append("🔴 高优先级关注")
        for i, it in enumerate(high_items, 1):
            lines.append(f"{i}. {it['title']}")
            lines.append(f"   摘要: {it['summary']}")
            lines.append(f"   来源: {it['source']} {it['url']} 【{SOURCE_TYPE_MAP.get(it.get('source_type'), '待核实')}】")
            if it.get("reason"):
                lines.append(f"   ⚡ 关注理由: {it['reason']}")

    # 各板块
    for cat_key, cat_name in CATEGORY_MAP.items():
        cat_items = [it for it in items if it.get("category") == cat_key]
        lines.append("")
        lines.append("━━━━━━━━━━━━━━━━━━")
        lines.append(cat_name)
        if not cat_items:
            lines.append("本板块今日无重大动态")
            continue
        for i, it in enumerate(cat_items, 1):
            priority = PRIORITY_MAP.get(it.get("priority"), "⚪ 常规")
            lines.append(f"{i}. {it['title']} {priority}")
            lines.append(f"   摘要: {it['summary']}")
            lines.append(f"   来源: {it['source']} {it['url']} 【{SOURCE_TYPE_MAP.get(it.get('source_type'), '待核实')}】")

    # 信息采集异常
    if failed:
        lines.append("")
        lines.append("━━━━━━━━━━━━━━━━━━")
        lines.append("⚠️ 信息采集异常")
        for name in failed:
            lines.append(f"- {name}: 因网络/服务原因暂未能获取, 将于30分钟后重试")

    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━")
    lines.append(f"📅 数据覆盖: {cover_label} 00:00-24:00")
    source_count = len(RSS_FEEDS) + 4  # RSS源 + 4个API源
    lines.append(f"📡 信息源: {source_count} 个(官方博客/权威媒体/学术平台/社区)")
    lines.append(f"🤖 由 GitHub Actions 自动生成")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 飞书推送(支持加签)
# ---------------------------------------------------------------------------
def send_to_feishu(text):
    """发送文本消息到飞书群机器人, 失败重试3次"""
    if not FEISHU_WEBHOOK:
        raise RuntimeError("未配置 FEISHU_WEBHOOK 环境变量")

    # 飞书单条文本消息上限约 30000 字符, 超长截断
    if len(text) > 28000:
        text = text[:28000] + "\n\n...(内容过长, 已截断)"
        log("  日报内容超长, 已截断至 28000 字符")

    payload = {"msg_type": "text", "content": {"text": text}}
    params = None
    if FEISHU_SECRET:
        timestamp = str(int(time.time()))
        string_to_sign = f"{timestamp}\n{FEISHU_SECRET}"
        hmac_code = hmac.new(string_to_sign.encode("utf-8"), digestmod=hashlib.sha256).digest()
        sign = base64.b64encode(hmac_code).decode("utf-8")
        params = {"timestamp": timestamp, "sign": sign}

    for attempt in range(3):
        try:
            resp = requests.post(FEISHU_WEBHOOK, params=params, json=payload, timeout=15)
            data = resp.json()
            if data.get("code") == 0:
                log("飞书推送成功")
                return True
            log(f"飞书推送失败(第{attempt + 1}次): {data}")
        except Exception as e:
            log(f"飞书推送异常(第{attempt + 1}次): {e}")
        time.sleep(3)
    return False


# ---------------------------------------------------------------------------
# 失败源重采
# ---------------------------------------------------------------------------
def retry_failed_sources(failed_names, start_date, end_date):
    """对指定失败源重新采集, 返回新增 items 列表和仍然失败的源列表"""
    collectors = get_all_collectors()
    new_items = []
    still_failed = []

    for name in failed_names:
        c = next((x for x in collectors if x["name"] == name), None)
        if not c:
            still_failed.append(name)
            continue
        try:
            log(f"  重采: {name}")
            if c["type"] == "rss":
                result = c["func"](*c["args"], start_date, end_date)
            else:
                result = c["func"](start_date, end_date)
            new_items.extend(result)
            log(f"  重采成功: {name} ({len(result)} 条)")
        except Exception as e:
            log(f"  重采失败: {name}: {e}")
            still_failed.append(name)

    return new_items, still_failed


# ---------------------------------------------------------------------------
# 主函数(单次执行, 内部完成 30 分钟重采)
# ---------------------------------------------------------------------------
def main():
    log("===== 每日AI资讯日报开始 =====")

    # 校验必填配置
    if not FEISHU_WEBHOOK:
        log("错误: 未配置 FEISHU_WEBHOOK")
        return {"code": 1, "msg": "FEISHU_WEBHOOK 未配置"}
    if not LLM_API_KEY:
        log("错误: 未配置 LLM_API_KEY")
        return {"code": 1, "msg": "LLM_API_KEY 未配置"}

    # 时间窗口: 前一天 00:00:00 ~ 当天 00:00:00 (北京时间)
    now = beijing_now()
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = today - timedelta(days=1)
    end_date = today
    cover_label = start_date.strftime("%Y-%m-%d")
    log(f"数据窗口: {cover_label} 00:00-24:00")

    # ========== 第一阶段: 全量采集 + 推送日报 ==========
    items, failed = collect_all(start_date, end_date)
    log(f"首轮采集完成: 共 {len(items)} 条, 失败源: {failed or '无'}")

    # LLM 摘要分级(主->备用->规则降级)
    if items:
        structured = summarize_with_llm(items)
        if structured is None:
            log("LLM 全部失败(主+备用), 使用规则降级")
            structured = fallback_format(items)
        report = build_report(structured, failed, cover_label)
    else:
        report = build_report([], failed, cover_label)

    # 推送飞书
    ok = send_to_feishu(report)
    if ok:
        log("首轮日报推送成功")
    else:
        log("首轮日报推送失败")

    # ========== 第二阶段: 如有失败源, 等待 30 分钟后重采 ==========
    if not failed:
        log("无失败源, 任务完成")
        log("===== 每日AI资讯日报执行成功 =====")
        return {"code": 0, "msg": "success", "items": len(items), "failed": [], "supplement": False}

    log(f"有 {len(failed)} 个源采集失败: {failed}")
    log(f"等待 {RETRY_WAIT_SECONDS} 秒(30分钟)后重采失败源...")
    time.sleep(RETRY_WAIT_SECONDS)

    # 重采失败源
    supplement_items, still_failed = retry_failed_sources(failed, start_date, end_date)
    log(f"重采完成: 新增 {len(supplement_items)} 条, 仍失败: {still_failed or '无'}")

    # 有新增内容 -> 推送补充更新
    if supplement_items:
        structured = summarize_with_llm(supplement_items, is_supplement=True)
        if structured is None:
            log("补充更新 LLM 失败, 使用规则降级")
            structured = fallback_format(supplement_items)
        supplement_report = build_report(structured, still_failed, cover_label, is_supplement=True)
        ok2 = send_to_feishu(supplement_report)
        if ok2:
            log("补充更新推送成功")
        else:
            log("补充更新推送失败")
    else:
        log("重采无新增内容, 跳过补充推送")

    # 如仍有失败源, 推送最终失败通知
    if still_failed:
        final_msg = (
            f"⚠️ AI资讯日报最终采集状态 | {cover_label}\n\n"
            f"以下信息源经30分钟重试后仍不可用:\n"
        )
        for name in still_failed:
            final_msg += f"- {name}\n"
        final_msg += "\n不再进行第三次重试。如需手动查看, 请访问对应网站。"
        send_to_feishu(final_msg)
        log(f"已推送最终失败通知: {still_failed}")

    log("===== 每日AI资讯日报执行成功 =====")
    return {
        "code": 0, "msg": "success",
        "items": len(items), "failed": failed,
        "supplement_items": len(supplement_items),
        "still_failed": still_failed,
        "supplement": len(supplement_items) > 0,
    }


if __name__ == "__main__":
    main()
