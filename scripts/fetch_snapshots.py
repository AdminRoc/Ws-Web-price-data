#!/usr/bin/env python3
"""Ws-Web-price-data · 快照抓取(Phase 1) —— 全量均价快照,每次运行一个批次。

口径(与 Public-WM fetch_avg_prices.py 一致):
  - 样本 = in-game + online 的卖单合并(/v2/orders/item/{slug});offline 永不参与
  - count>=3:去掉最低价(第 1 位),取第 2 与第 3 位价格均值 = avg
  - count 1~2:全部价格取平均
  - count=0:avg=null(不参与当日平均,聚合侧跳过)
  - special = count<3
  - 可分级物品(maxRank>0,如 Mod/Arcane)额外输出 avg_zero(0级)/avg_max(满级)/
    max_rank —— 满级与零级价格往往差数倍,分开统计避免信息丢失。

速率(遵守 wfm-docs 官方规则,How-To-Design-The-UI/wfm-docs/docs/rules/overview.md):
  - 公共 API 限速 3 req/s:全局 RateLimiter 强制,任意窗口请求发起速率 ≤3/s;
  - 并发 8 仅容忍慢响应,不提高请求发起速率。

产物:
  - data/snapshots/{UTC+8当日}.json —— 每日一个文件,内含 batches 数组,本次追加一个批次:
      { date, tz, generated, batches: [ { time, items: { slug: {avg,count,used,special[,avg_zero,avg_max,max_rank]} } } ] }
  - data/meta/items.json —— 物品清单(名称/中文名/类别/maxRank),供站点搜索与筛选

抗劣化设计(2026-08-19 实测 WM 间歇性慢响应后加固):
  - Retry-After 优先(429/509 按响应头等待);
  - 退避封顶 30s;单物品最多 4 次尝试;
  - 自适应限速:滚动窗口失败率 >50% 时自动加大请求间隔,恢复后回落;
  - 总时间预算 RUN_TIME_BUDGET:到期中断本批,提交已抓到的部分批次(宁可部分,不可空跑);
  - 未抓到的物品不写入批次(由聚合侧按实际样本统计)。

时区(用户要求精准换算):
  - 时间戳一律存 UTC ISO 8601;
  - 日期键按 UTC+8(Asia/Shanghai,无夏令时,恒 +8h)换算 —— 用固定 offset,不依赖 tzdata。

环境变量:
  - DATA_DIR  默认 "data"
  - MAX_ITEMS 默认 0=全量;>0 仅取前 N 个(本地冒烟测试用)
  - MAX_RPS   默认 3.0(wfm-docs 限速,不建议调高)
"""
import asyncio
import json
import os
import random
import sys
import time
from collections import deque
from datetime import datetime, timezone, timedelta

import aiohttp

DIRECT_URL = "https://api.warframe.market"

# 速率策略(遵守 wfm-docs 官方文档规则 — How-To-Design-The-UI/wfm-docs/docs/rules/overview.md):
#   "The general public API limit is 3 requests per second."
# - 全局令牌式限速器强制 ≤3 req/s(无论并发多少,请求"发起"速率恒定 ≤3/s);
# - 并发 8 仅用于容忍慢响应(在途请求数),不提高请求发起速率;
# - 全量 3837 物品理论最短耗时 ≈ 3837/3 ≈ 21 分钟,预算 35 分钟留足重试余量;
# - 自适应限速仍保留:失败率高时进一步加大间隔,恢复后回落。
CONCURRENCY = int(os.environ.get("CONCURRENCY", "8"))
MAX_RPS = float(os.environ.get("MAX_RPS", "3.0"))   # wfm-docs 规定的公共 API 限速
STARTUP_JITTER_MAX = 180  # 3min,错峰
BATCH_SIZE = 50
BATCH_PAUSE_MIN = 2
BATCH_PAUSE_MAX = 5
MAX_RETRIES = 4          # 单物品尝试次数(含首次)
BACKOFF_CAP = 30         # 秒,指数退避封顶
RETRY_ROUNDS = 2         # 主循环后对失败 slug 的补跑轮数
RUN_TIME_BUDGET = int(os.environ.get("RUN_TIME_BUDGET", "2100"))  # 秒(35min),到期中断并提交部分批次
ITEMS_TIMEOUT = 30
ITEMS_RETRIES = 5
PROGRESS_EVERY = 300

DATA_DIR = os.environ.get("DATA_DIR", "data")
SNAPSHOTS_DIR = os.path.join(DATA_DIR, "snapshots")
ITEMS_OUT = os.path.join(DATA_DIR, "meta", "items.json")
MAX_ITEMS = int(os.environ.get("MAX_ITEMS", "0") or 0)

TZ_CN = timezone(timedelta(hours=8))  # Asia/Shanghai = UTC+8,无夏令时

HEADERS = {
    "User-Agent": "ws-web-price-snapshot-bot/1.0 (+https://github.com/AdminRoc/Ws-Web-price-data; hourly avg price snapshots)",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Platform": "pc",
    "Language": "zh-hans",
    "Origin": "https://warframe.market",
    "Referer": "https://warframe.market/",
}


class Throttle:
    """自适应限速:滚动窗口统计失败率,失败率高时加大请求间隔,恢复后回落。"""

    def __init__(self, max_rps):
        self.max_rps = max_rps
        self.cur_delay = 0.0
        self.window = deque(maxlen=60)

    def record(self, ok):
        self.window.append(1 if ok else 0)
        if len(self.window) >= 20:
            rate = sum(self.window) / len(self.window)
            if rate < 0.5:
                self.cur_delay = min(2.0, self.cur_delay + 0.2)
            elif rate > 0.85:
                self.cur_delay = max(0.0, self.cur_delay - 0.1)

    def jitter(self):
        return self.cur_delay + random.random() * 0.05


class RateLimiter:
    """全局请求速率限制器(wfm-docs 规则:公共 API ≤3 req/s)。
    在每次请求"发起"前 wait(),保证任意窗口内请求发起速率不超过 MAX_RPS。"""

    def __init__(self, max_rps):
        self.interval = 1.0 / max_rps
        self._lock = asyncio.Lock()
        self._next = 0.0

    async def wait(self):
        async with self._lock:
            now = time.monotonic()
            if now < self._next:
                await asyncio.sleep(self._next - now)
                now = time.monotonic()
            self._next = now + self.interval


def _utc_now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _cn_date():
    return datetime.now(TZ_CN).strftime("%Y-%m-%d")


def _backoff(attempt, resp=None):
    if resp is not None:
        try:
            ra = int(resp.headers.get("Retry-After", "0"))
            if ra > 0:
                return min(ra + random.random() * 2, 120)
        except Exception:
            pass
    return min((2 ** attempt) * 5, BACKOFF_CAP) + random.random() * 2


def calc_avg(orders, rank=None, order_type="sell"):
    """合并口径均价(去最低,取第 2/3 位均值)。
    rank=None 为整体口径;rank=数字 仅统计该等级卖单(用于 avg_zero/avg_max)。
    order_type='sell' 统计卖单;'buy' 统计求购单(同样 in-game+online 样本,供未来价差统计)。"""
    prices = sorted(
        int(o["platinum"]) for o in orders
        if (o.get("type") or o.get("order_type") or o.get("orderType") or "").lower() == order_type
        and o.get("visible", True) is not False
        and o.get("platinum", 0) > 0
        and (o.get("user") or {}).get("status", "").lower() in ("ingame", "online")
        and (rank is None or o.get("rank") == rank)
    )
    count = len(prices)
    if count == 0:
        return {"avg": None, "count": 0, "used": 0, "special": True}
    if count >= 3:
        avg = round((prices[1] + prices[2]) / 2)
        used = 2
    else:
        avg = round(sum(prices) / count)
        used = count
    result = {"avg": avg, "count": count, "used": used}
    if count < 3:
        result["special"] = True
    return result


CATEGORY_PREF = [
    "warframe", "primary", "secondary", "melee", "archwing",
    "mods", "mod", "arcane", "relic", "component", "fish",
    "gem", "misc", "sentinel", "pet", "companion",
]


def _category(tags):
    """WM 官方 tags 按优先序取主类别(与站点 data-source.js categoryOf 一致);
    无命中时退回 tags 首项,再退回 other。"""
    list_tags = tags or []
    for pref in CATEGORY_PREF:
        for t in list_tags:
            if str(t).lower() == pref:
                return pref
    return (list_tags[0] if list_tags else "other") or "other"


def _item_meta(it, previous=None):
    previous = previous or {}
    i18n = it.get("i18n") or {}
    zh = (i18n.get("zh-hans") or i18n.get("zh") or {}).get("name") or ""
    en = (i18n.get("en") or {}).get("name") or it.get("item_name") or it.get("url_name") or ""
    return {
        "name": en or previous.get("name") or it.get("slug"),
        "name_zh": zh or previous.get("name_zh") or en or it.get("slug"),
        "wm_deleted": False,
        "category": _category(it.get("tags")),
        "tags": it.get("tags") or [],
        "maxRank": it.get("maxRank") or 0,
        # 未来展示/统计可能需要的元数据(来自 /v2/items,一次性补齐,避免缺数据)
        "thumb": it.get("thumb") or "",
        "icon": it.get("icon") or "",
        "rarity": it.get("rarity"),
        "bulkTradable": bool(it.get("bulkTradable")),
        "tradingTax": it.get("tradingTax"),
        "maxCharges": it.get("maxCharges"),
    }


async def fetch_items(session):
    """全量物品清单,指数退避重试;失败会中断本次运行(workflow 自动重试)。"""
    url = f"{DIRECT_URL}/v2/items"
    last = None
    for attempt in range(ITEMS_RETRIES):
        try:
            async with session.get(url, headers=HEADERS,
                                   timeout=aiohttp.ClientTimeout(total=ITEMS_TIMEOUT)) as r:
                if r.status != 200:
                    last = RuntimeError(f"GET {url} -> HTTP {r.status}")
                    raise last
                return await r.json(content_type=None)
        except Exception as e:
            last = e
            if attempt < ITEMS_RETRIES - 1:
                await asyncio.sleep(_backoff(attempt))
                continue
            raise
    raise last


async def fetch_slug(session, sem, limiter, slug, max_rank, throttle, deadline):
    """单物品抓取:返回 (slug, 结果) / (slug, "ERROR") / (slug, "SKIPPED")。
    每次请求发起前经 RateLimiter 限速(≤3 req/s);尝试前检查总截止时间(deadline)。"""
    url = f"{DIRECT_URL}/v2/orders/item/{slug}"
    async with sem:
        for attempt in range(MAX_RETRIES):
            if time.time() > deadline:
                return slug, "SKIPPED"
            try:
                await limiter.wait()   # wfm-docs:公共 API ≤3 req/s
                async with session.get(url, headers=HEADERS,
                                       timeout=aiohttp.ClientTimeout(total=20)) as r:
                    if r.status in (429, 509):
                        throttle.record(False)
                        await asyncio.sleep(_backoff(attempt, r))
                        continue
                    if r.status != 200:
                        throttle.record(False)
                        if attempt < MAX_RETRIES - 1:
                            await asyncio.sleep(1 + random.random())
                            continue
                        return slug, "ERROR"
                    data = await r.json(content_type=None)
                    orders = data.get("data") or []
                    throttle.record(True)
                    await asyncio.sleep(throttle.jitter())
                    result = calc_avg(orders)
                    result["total"] = len(orders)   # 全部订单数(含 offline/求购,市场活跃度代理指标)
                    # 求购均价(价差统计用,与卖单同口径但取 buy 单)
                    buy = calc_avg(orders, order_type="buy")
                    if buy.get("avg") is not None:
                        result["buy_avg"] = buy
                    # 可分级物品:额外统计 0级(avg_zero)与满级(avg_max)口径,统计不丢等级信息
                    if max_rank and max_rank > 0:
                        result["avg_zero"] = calc_avg(orders, rank=0)
                        result["avg_max"] = calc_avg(orders, rank=max_rank)
                        result["max_rank"] = max_rank
                    return slug, result
            except Exception:
                throttle.record(False)
                await asyncio.sleep(0.5 + random.random())
    return slug, "ERROR"


async def run_pass(session, sem, limiter, tasks, max_ranks, results, failed, round_no, throttle, t0):
    """执行一轮抓取;round_no=0 全量,round_no>=1 只处理 failed。
    总时间预算到期时中断剩余任务,返回已完成的(部分批次)。"""
    pending = tasks if round_no == 0 else list(failed)
    if not pending:
        return False
    deadline = t0 + RUN_TIME_BUDGET
    tasks_ = [asyncio.ensure_future(
        fetch_slug(session, sem, limiter, slug, max_ranks.get(slug, 0), throttle, deadline))
        for slug in pending]
    done_count = 0
    failed.clear()
    timed_out = False
    remaining = list(tasks_)
    while remaining:
        now = time.time()
        if now > deadline:
            timed_out = True
            break
        done, remaining = await asyncio.wait(
            remaining, timeout=max(0.1, deadline - now),
            return_when=asyncio.FIRST_COMPLETED)
        for t in done:
            done_count += 1
            slug, result = t.result()   # fetch_slug 内部全捕获,不会抛异常
            if result == "ERROR":
                failed.add(slug)
                continue
            if result == "SKIPPED":
                continue
            results[slug] = result
            if done_count % PROGRESS_EVERY == 0:
                elapsed = time.time() - t0
                print(f"  [进度 {done_count}/{len(pending)}] 耗时 {elapsed/60:.1f} 分钟,"
                      f"失败 {len(failed)},当前间隔 {throttle.cur_delay:.2f}s", flush=True)
            if done_count % BATCH_SIZE == 0 and done_count < len(pending):
                await asyncio.sleep(BATCH_PAUSE_MIN + random.random() * (BATCH_PAUSE_MAX - BATCH_PAUSE_MIN))
    if timed_out:
        for t in remaining:
            t.cancel()
        await asyncio.gather(*remaining, return_exceptions=True)
        print(f"  ⏰ 超过 {RUN_TIME_BUDGET/60:.0f} 分钟预算,中断本轮,提交部分批次", flush=True)
    print(f"  第{round_no}轮:完成 {done_count},失败 {len(failed)}"
          + (f",补跑剩余 {len(failed)}" if failed else ""), flush=True)
    return timed_out


def append_snapshot(date, batch_items, generated, planned_items, error_items):
    """向当日快照文件追加一个批次,保留质量与可回放信息。"""
    os.makedirs(SNAPSHOTS_DIR, exist_ok=True)
    snap_path = os.path.join(SNAPSHOTS_DIR, f"{date}.json")
    data = {}
    if os.path.exists(snap_path):
        try:
            with open(snap_path, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
        except Exception:
            data = {}
    batches = data.get("batches") or []
    batches.append({"time": generated, "planned_items": planned_items, "captured_items": len(batch_items), "error_items": error_items, "items": batch_items})
    data["schema_version"] = SCHEMA_VERSION
    data["generator_version"] = GENERATOR_VERSION
    data["data_revision"] = DATA_REVISION
    data["model_version"] = MODEL_VERSION
    data["date"] = date
    data["tz"] = "Asia/Shanghai (UTC+8)"
    data["generated"] = generated
    data["batches"] = batches
    with open(snap_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    print(f"  已追加批次 → {snap_path} (共 {len(batches)} 个批次,{len(batch_items)} 物品)", flush=True)


def write_items_manifest(items_meta):
    os.makedirs(os.path.dirname(ITEMS_OUT), exist_ok=True)
    manifest = {
        "generated": _utc_now_iso(),
        "source": DIRECT_URL + "/v2/items",
        "items": items_meta,
    }
    with open(ITEMS_OUT, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, separators=(",", ":"))
    print(f"  已写入物品清单 → {ITEMS_OUT} ({len(items_meta)} 物品)", flush=True)


async def main():
    if MAX_ITEMS <= 0:
        startup = random.randint(0, STARTUP_JITTER_MAX)
        if startup > 0:
            print(f"启动随机延迟 {startup / 60:.1f} 分钟,避免固定时刻脉冲...", flush=True)
            await asyncio.sleep(startup)
    else:
        print(f"[冒烟测试] MAX_ITEMS={MAX_ITEMS}", flush=True)

    print("正在获取全量物品列表...", flush=True)
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(
            limit=CONCURRENCY, limit_per_host=CONCURRENCY, ttl_dns_cache=300)) as session:
        items_data = await fetch_items(session)

    items = [it for it in (items_data.get("data") or []) if it.get("slug")]
    if MAX_ITEMS > 0:
        items = items[:MAX_ITEMS]
    slugs = [it["slug"] for it in items]
    # maxRank>0 才是真正可分级物品(mod/arcane 等),用于 avg_zero/avg_max 拆分
    max_ranks = {it["slug"]: (it.get("maxRank") or 0) for it in items}
    est = len(slugs) / MAX_RPS / 60
    print(f"共 {len(slugs)} 个物品,并发={CONCURRENCY},限速 {MAX_RPS} req/s(wfm-docs 规则),"
          f"预估 {est:.1f} 分钟,预算 {RUN_TIME_BUDGET/60:.0f} 分钟", flush=True)

    results = {}
    failed = set()
    sem = asyncio.Semaphore(CONCURRENCY)
    throttle = Throttle(MAX_RPS)
    limiter = RateLimiter(MAX_RPS)
    t0 = time.time()

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(
            limit=CONCURRENCY, limit_per_host=CONCURRENCY, ttl_dns_cache=300)) as session:
        timed_out = await run_pass(session, sem, limiter, slugs, max_ranks, results, failed, 0, throttle, t0)
        if not timed_out:
            for rnd in range(1, RETRY_ROUNDS + 1):
                if not failed:
                    break
                timed_out = await run_pass(session, sem, limiter, slugs, max_ranks, results, failed, rnd, throttle, t0)
                if timed_out:
                    break

    # 补跑仍失败:保留为 avg:null(聚合侧跳过),保证结构完整
    for slug in failed:
        results[slug] = {"avg": None, "count": 0, "used": 0, "special": True, "error": True}

    has_avg = sum(1 for v in results.values() if v.get("avg"))
    elapsed = time.time() - t0
    print(f"抓取完成!有均价:{has_avg}  共:{len(results)}/{len(slugs)}  耗时:{elapsed / 60:.1f} 分钟", flush=True)

    date = _cn_date()
    generated = _utc_now_iso()
    batch_items = {}
    for it in items:
        slug = it["slug"]
        if slug in results:
            batch_items[slug] = results[slug]
    error_items = sum(1 for value in results.values() if value.get("error"))
    append_snapshot(date, batch_items, generated, len(items), error_items)

    previous_items = (load_json(ITEMS_OUT) or {}).get("items") or {}
    items_meta = {it["slug"]: _item_meta(it, previous_items.get(it["slug"])) for it in items}
    for slug, previous in previous_items.items():
        if slug not in items_meta:
            retained = dict(previous)
            retained["wm_deleted"] = True
            items_meta[slug] = retained
    write_items_manifest(items_meta)


if __name__ == "__main__":
    asyncio.run(main())
