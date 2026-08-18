#!/usr/bin/env python3
"""Ws-Web-price-data · 日均价聚合(Phase 2) —— 前一日快照 → 日均价 → 系列 → 表格 bundle。

语义(用户需求):
  - 日界 = UTC+8(Asia/Shanghai,恒 +8h 无夏令时);时间戳存 UTC ISO,日期键按 UTC+8;
  - 每次都在“后一日 0 点之后”把时间戳位于前一日范围内的所有均价数据聚合成前一日日均价
    (例如 2026-08-18 的日均价,由 2026-08-18 当天全部快照批次聚合);
  - “临时当日均价”:今日 0 点至现在的全部快照均值(用户查询时由站点/本产物提供);
  - 日均价滚动保留 1000 天;快照保留 7 天。

模式:
  - 默认(完整):聚合昨日日均价(幂等)→ 追加昨日到 series(幂等)→ 重建 table/latest.json
    (含今日临时均价与预测)→ 刷新 meta/latest.json → 清理过期文件;
  - --refresh-bundle:仅重建 table/latest.json + meta/latest.json(供每 2h 快照后同步当日临时均价)。

产物:
  - data/daily/{昨日}.json       { date, tz, generated, items: {slug: {avg,samples,valid,min,max,std}} }
  - data/series/{slug}.json      { slug, name, name_zh, category, days: [{d,avg,std,valid}...] } 保留1000天
  - data/table/latest.json       { generated, day, last_daily, items: {slug: {name,name_zh,category,
                                  today, ma3..ma90, std3..std90, chg3..chg90, short_pred, long_pred}} }
  - data/meta/latest.json        站点状态元数据(今日/最近日均价/天数/物品数/快照信息)
"""
import argparse
import json
import math
import os
import statistics
import sys
from datetime import datetime, timezone, timedelta

DATA_DIR = os.environ.get("DATA_DIR", "data")
SNAPSHOTS_DIR = os.path.join(DATA_DIR, "snapshots")
DAILY_DIR = os.path.join(DATA_DIR, "daily")
SERIES_DIR = os.path.join(DATA_DIR, "series")
TABLE_OUT = os.path.join(DATA_DIR, "table", "latest.json")
META_OUT = os.path.join(DATA_DIR, "meta", "latest.json")
ITEMS_IN = os.path.join(DATA_DIR, "meta", "items.json")

TZ_CN = timezone(timedelta(hours=8))  # Asia/Shanghai = UTC+8,无夏令时

SNAPSHOT_RETENTION_DAYS = 7
DAILY_RETENTION_DAYS = 1000
SERIES_RETENTION_DAYS = 1000
TABLE_MAX_DAYS = 180   # 表格 MA90 + chg90(双窗口)所需上限

# 预测模型参数(与站点 js/model-config.js 保持一致)
SHORT_WINDOWS = (3, 14)
LONG_WINDOWS = (30, 90)
EXTREME_VOL_RATIO = 1.5
BAND_SIGMA = 2.0


def _utc_now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _cn_date(dt=None):
    return (dt or datetime.now(TZ_CN)).strftime("%Y-%m-%d")


def _cn_date_days_ago(n):
    return (datetime.now(TZ_CN) - timedelta(days=n)).strftime("%Y-%m-%d")


def _r2(x):
    return round(x, 2) if x is not None else None


def _r4(x):
    return round(x, 4) if x is not None else None


def _stdev(vals):
    if len(vals) < 2:
        return None
    return statistics.stdev(vals)


def _mean(vals):
    return sum(vals) / len(vals) if vals else None


def load_json(path, default=None):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))


# ── 1. 日均价聚合(前一日)───────────────────────────────────────────────
def aggregate_daily(date, force=False):
    """聚合指定 UTC+8 日期的全部快照批次 → data/daily/{date}.json。幂等。"""
    out_path = os.path.join(DAILY_DIR, f"{date}.json")
    if os.path.exists(out_path) and not force:
        print(f"  [跳过] {date} 日均价已存在")
        return out_path, True

    snap_path = os.path.join(SNAPSHOTS_DIR, f"{date}.json")
    snap = load_json(snap_path)
    batches = (snap or {}).get("batches") or []
    if not batches:
        print(f"  [跳过] {date} 无快照文件或批次为空")
        return None, False

    items = {}
    for batch in batches:
        for slug, rec in (batch.get("items") or {}).items():
            items.setdefault(slug, []).append(rec)

    daily_items = {}
    for slug, recs in items.items():
        valid = [r["avg"] for r in recs if r.get("avg") is not None]
        samples = len(recs)
        v = len(valid)
        if v == 0:
            continue
        entry = {
            "avg": round(_mean(valid), 2),
            "samples": samples,
            "valid": v,
            "min": min(valid),
            "max": max(valid),
            "std": _r2(_stdev(valid)),
        }
        # 等级拆分汇总(快照携带 avg_zero/avg_max 时):统计不丢等级信息
        for key in ("avg_zero", "avg_max"):
            rv = [r[key]["avg"] for r in recs
                  if r.get(key) and r[key].get("avg") is not None]
            if rv:
                entry[key] = {"avg": round(_mean(rv), 2), "valid": len(rv)}
        daily_items[slug] = entry

    doc = {
        "date": date,
        "tz": "Asia/Shanghai (UTC+8)",
        "generated": _utc_now_iso(),
        "items": daily_items,
    }
    save_json(out_path, doc)
    print(f"  [聚合] {date} → {out_path} ({len(daily_items)} 物品)")
    return out_path, False


# ── 2. 系列追加(前一日)─────────────────────────────────────────────────
def append_series(date, items_meta):
    """把某日日均价追加进 data/series/{slug}.json(幂等:已含该日则跳过;超 1000 天裁剪)。"""
    daily = load_json(os.path.join(DAILY_DIR, f"{date}.json"))
    if not daily or not daily.get("items"):
        return 0
    n = 0
    for slug, entry in daily["items"].items():
        series_path = os.path.join(SERIES_DIR, f"{slug}.json")
        series = load_json(series_path)
        if series is None:
            meta = (items_meta or {}).get(slug) or {}
            series = {
                "slug": slug,
                "name": meta.get("name") or slug,
                "name_zh": meta.get("name_zh") or meta.get("name") or slug,
                "category": meta.get("category") or "other",
                "days": [],
            }
        days = series.get("days") or []
        if days and days[-1].get("d") == date:
            continue  # 已追加
        days.append({
            "d": date,
            "avg": entry["avg"],
            "std": entry.get("std"),
            "valid": entry.get("valid", 0),
        })
        # 滚动保留 1000 天
        if len(days) > SERIES_RETENTION_DAYS:
            days = days[-SERIES_RETENTION_DAYS:]
        series["days"] = days
        save_json(series_path, series)
        n += 1
    if n:
        print(f"  [系列] {date} 追加 {n} 个物品系列")
    return n


# ── 3. 预测模型(与 DESIGN §8 / 站点 js/model-config.js 一致)────────────
def _fmt(v):
    return f"{v:.2f}" if v is not None else "—"


def predict_short(P, ma3, ma14, std3, std14, ma30, std30, days):
    """短期预测优先级链:S0 极端震荡 → S1 短期均值回归 → S2 高位回调 → S3 均线交叉。"""
    need = 30
    if any(x is None for x in (ma3, ma14, std14, ma30, std30)):
        return {"label": "数据不足", "cls": "na",
                "detail": f"数据不足:当前累计 {days} 天,短期模型需 {need} 天"}
    # S0: 极端震荡检测(σ3 > 1.5 × σ30)
    if std3 is not None:
        if std30 == 0:
            if std3 > 0:
                return {"label": "震荡剧烈", "cls": "swing",
                        "detail": f"近3日波幅 {_fmt(std3)} 而30日波幅为 0,市场骤然异动,均线信号失效"}
        elif std3 > EXTREME_VOL_RATIO * std30:
            return {"label": "震荡剧烈", "cls": "swing",
                    "detail": f"近3日波幅 {_fmt(std3)} 是30日波幅 {_fmt(std30)} 的 "
                              f"{std3 / std30:.2f} 倍,市场分歧极大,均线信号失效"}
    # S1: 短期均值回归(14 日波动带)
    upper14 = ma14 + BAND_SIGMA * std14
    lower14 = ma14 - BAND_SIGMA * std14
    if P > upper14:
        return {"label": "强烈看跌", "cls": "down",
                "detail": f"最新价 {_fmt(P)} 突破14日波动上轨(MA14 {_fmt(ma14)} + 2σ {_fmt(2 * std14)} "
                          f"= {_fmt(upper14)}),严重超买,回归压力大"}
    if P < lower14:
        return {"label": "强烈看涨", "cls": "up",
                "detail": f"最新价 {_fmt(P)} 跌破14日波动下轨(MA14 {_fmt(ma14)} − 2σ {_fmt(2 * std14)} "
                          f"= {_fmt(lower14)}),严重超卖,均值回归预期强"}
    # S2: 高位回调风险(30 日波动上轨)
    upper30 = ma30 + BAND_SIGMA * std30
    if P > upper30:
        return {"label": "短期看跌回调", "cls": "down",
                "detail": f"最新价 {_fmt(P)} 突破30日波动上轨(MA30 {_fmt(ma30)} + 2σ30 {_fmt(2 * std30)} "
                          f"= {_fmt(upper30)}),超买回调动能大"}
    # S3: 短期均线交叉
    if ma3 > ma14:
        return {"label": "温和看涨", "cls": "up",
                "detail": f"MA3 {_fmt(ma3)} > MA14 {_fmt(ma14)},短期均线多头,价格位于波动带内"}
    return {"label": "温和看跌", "cls": "down",
            "detail": f"MA3 {_fmt(ma3)} < MA14 {_fmt(ma14)},短期均线空头,价格位于波动带内"}


def predict_long(P, ma30, std30, ma90, std90, days):
    """长期预测优先级链:L1 长期均值回归 → L2 长期均线交叉。"""
    need = 90
    if any(x is None for x in (ma30, std30, ma90, std90)):
        return {"label": "数据不足", "cls": "na",
                "detail": f"数据不足:当前累计 {days} 天,长期模型需 {need} 天"}
    upper90 = ma90 + BAND_SIGMA * std90
    lower90 = ma90 - BAND_SIGMA * std90
    if P > upper90:
        return {"label": "看跌·泡沫", "cls": "down",
                "detail": f"最新价 {_fmt(P)} 突破90日波动上轨(MA90 {_fmt(ma90)} + 2σ90 {_fmt(2 * std90)} "
                          f"= {_fmt(upper90)}),存在较大价格泡沫"}
    if P < lower90:
        return {"label": "看涨·低估", "cls": "up",
                "detail": f"最新价 {_fmt(P)} 跌破90日波动下轨(MA90 {_fmt(ma90)} − 2σ90 {_fmt(2 * std90)} "
                          f"= {_fmt(lower90)}),价值被严重低估"}
    if ma30 > ma90:
        return {"label": "看涨", "cls": "up",
                "detail": f"MA30 {_fmt(ma30)} > MA90 {_fmt(ma90)},资金长期持续流入"}
    return {"label": "看跌", "cls": "down",
            "detail": f"MA30 {_fmt(ma30)} < MA90 {_fmt(ma90)},资金长期持续流出"}


# ── 4. 表格 bundle ─────────────────────────────────────────────────────
def build_table_bundle(items_meta):
    """重建 data/table/latest.json:全物品多窗口均价/波幅/环比 + 短长期预测。"""
    # 物品 → 日均价序列(最近 TABLE_MAX_DAYS 天,按日期升序)
    daily_files = []
    if os.path.isdir(DAILY_DIR):
        daily_files = sorted(f for f in os.listdir(DAILY_DIR) if f.endswith(".json"))
    daily_files = daily_files[-TABLE_MAX_DAYS:]

    series_by_slug = {}
    for fname in daily_files:
        date = fname[:-5]
        doc = load_json(os.path.join(DAILY_DIR, fname))
        if not doc:
            continue
        for slug, e in (doc.get("items") or {}).items():
            if e.get("avg") is None:
                continue
            series_by_slug.setdefault(slug, []).append((date, e["avg"]))

    # 今日快照 → 临时当日均价
    today = _cn_date()
    snap_today = load_json(os.path.join(SNAPSHOTS_DIR, f"{today}.json"))
    today_series = {}
    if snap_today:
        for batch in (snap_today.get("batches") or []):
            for slug, rec in (batch.get("items") or {}).items():
                if rec.get("avg") is not None:
                    today_series.setdefault(slug, []).append(rec["avg"])

    last_daily = daily_files[-1][:-5] if daily_files else None
    items = {}
    all_slugs = set(series_by_slug) | set(today_series)
    for slug in all_slugs:
        meta = (items_meta or {}).get(slug) or {}
        vals = [v for _, v in series_by_slug.get(slug, [])]          # 日均价序列
        today_avgs = today_series.get(slug)
        today_entry = None
        if today_avgs:
            today_entry = {
                "avg": round(_mean(today_avgs), 2),
                "std": _r2(_stdev(today_avgs)),
                "valid": len(today_avgs),
            }
        last_avg = vals[-1] if vals else None          # 前一天日均价(区间筛选用)
        values = vals + ([today_entry["avg"]] if today_entry else [])
        if not values:
            continue
        P = values[-1]

        def _win(n):
            if len(values) < n:
                return None, None
            return _mean(values[-n:]), _stdev(values[-n:])

        def _chg(n):
            if len(values) < 2 * n:
                return None
            cur = _mean(values[-n:])
            prev = _mean(values[-2 * n:-n])
            if not cur or not prev:
                return None
            return (cur - prev) / prev

        ma3, std3 = _win(3)
        ma7, std7 = _win(7)
        ma14, std14 = _win(14)
        ma30, std30 = _win(30)
        ma60, std60 = _win(60)
        ma90, std90 = _win(90)
        days = len(values)

        short_pred = predict_short(P, ma3, ma14, std3, std14, ma30, std30, days)
        long_pred = predict_long(P, ma30, std30, ma90, std90, days)

        entry = {
            "name": meta.get("name") or slug,
            "name_zh": meta.get("name_zh") or meta.get("name") or slug,
            "category": meta.get("category") or "other",
            "today": today_entry,
            "last_avg": _r2(last_avg),
            "ma3": _r2(ma3), "std3": _r2(std3), "chg3": _r4(_chg(3)),
            "ma7": _r2(ma7), "std7": _r2(std7), "chg7": _r4(_chg(7)),
            "ma14": _r2(ma14), "std14": _r2(std14), "chg14": _r4(_chg(14)),
            "ma30": _r2(ma30), "std30": _r2(std30), "chg30": _r4(_chg(30)),
            "ma60": _r2(ma60), "std60": _r2(std60), "chg60": _r4(_chg(60)),
            "ma90": _r2(ma90), "std90": _r2(std90), "chg90": _r4(_chg(90)),
            "short_pred": short_pred,
            "long_pred": long_pred,
        }
        items[slug] = entry

    bundle = {
        "generated": _utc_now_iso(),
        "tz": "Asia/Shanghai (UTC+8)",
        "day": today,
        "last_daily": last_daily,
        "items": items,
    }
    save_json(TABLE_OUT, bundle)
    print(f"  [bundle] → {TABLE_OUT} ({len(items)} 物品)")
    return bundle


# ── 5. 元数据 ──────────────────────────────────────────────────────────
def build_meta(bundle, daily_files, items_meta):
    today = _cn_date()
    daily_dates = sorted(f[:-5] for f in daily_files if f.endswith(".json"))
    snap_today = load_json(os.path.join(SNAPSHOTS_DIR, f"{today}.json"))
    last_snapshot = None
    batches_today = 0
    if snap_today:
        batches_today = len(snap_today.get("batches") or [])
        bts = snap_today.get("batches") or []
        if bts:
            last_snapshot = bts[-1].get("time")
    meta = {
        "generated": _utc_now_iso(),
        "tz": "Asia/Shanghai (UTC+8)",
        "today": today,
        "last_snapshot": last_snapshot,
        "snapshot_batches_today": batches_today,
        "last_daily": daily_dates[-1] if daily_dates else None,
        "daily_count": len(daily_dates),
        "oldest_daily": daily_dates[0] if daily_dates else None,
        "newest_daily": daily_dates[-1] if daily_dates else None,
        "total_items": len(items_meta or {}),
        "table_items": len((bundle or {}).get("items") or {}),
    }
    save_json(META_OUT, meta)
    print(f"  [meta] → {META_OUT}")
    return meta


# ── 6. 保留清理 ────────────────────────────────────────────────────────
def cleanup():
    today = _cn_date()
    removed = 0
    if os.path.isdir(SNAPSHOTS_DIR):
        cutoff = _cn_date_days_ago(SNAPSHOT_RETENTION_DAYS)
        for f in os.listdir(SNAPSHOTS_DIR):
            if f.endswith(".json") and f[:-5] < cutoff:
                os.remove(os.path.join(SNAPSHOTS_DIR, f))
                removed += 1
    if os.path.isdir(DAILY_DIR):
        cutoff = _cn_date_days_ago(DAILY_RETENTION_DAYS)
        for f in os.listdir(DAILY_DIR):
            if f.endswith(".json") and f[:-5] < cutoff:
                os.remove(os.path.join(DAILY_DIR, f))
                removed += 1
    if removed:
        print(f"  [清理] 移除 {removed} 个过期文件")


# ── 入口 ───────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--refresh-bundle", action="store_true",
                        help="仅重建表格 bundle 与 meta(每 2h 快照后调用)")
    parser.add_argument("--force", action="store_true", help="强制重算昨日日均价")
    args = parser.parse_args()

    items_meta = (load_json(ITEMS_IN) or {}).get("items") or {}
    today = _cn_date()

    daily_files = []
    if os.path.isdir(DAILY_DIR):
        daily_files = sorted(f for f in os.listdir(DAILY_DIR) if f.endswith(".json"))

    if not args.refresh_bundle:
        # 昨日 = 今日 UTC+8 减 1 天(时间戳可能跨 0 点,按 UTC+8 归属日聚合)
        yesterday = (datetime.now(TZ_CN) - timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"== 日均价聚合(目标日 {yesterday},今日 {today}) ==", flush=True)
        _, already = aggregate_daily(yesterday, force=args.force)
        if not already:
            append_series(yesterday, items_meta)
        cleanup()
        if os.path.isdir(DAILY_DIR):
            daily_files = sorted(f for f in os.listdir(DAILY_DIR) if f.endswith(".json"))

    print("== 重建表格 bundle ==", flush=True)
    bundle = build_table_bundle(items_meta)
    build_meta(bundle, daily_files, items_meta)
    print("完成", flush=True)


if __name__ == "__main__":
    main()
