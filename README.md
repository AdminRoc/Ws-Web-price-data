# Ws-Web-price-data

**Ws-Web-price 的公开数据仓** —— 全部数据 action 在此运作(公库 action 分钟数不限),产物提交进 `data/`,经 **jsDelivr 反向引用**到站点 `Ws-Web-price`(`price.wfspeed.run`),与 relic 模块(`Ws-Web-relic-data` ↔ `Ws-Web-relic`)同构。

## 数据管线

| 工作流 | 触发 | 产物 |
|---|---|---|
| `fetch-snapshots.yml` | 每 2h(UTC 第 15 分)+ 手动 | `data/snapshots/YYYY-MM-DD.json`(当日快照批次)+ `data/meta/items.json` |
| `aggregate-daily.yml`(Phase 2) | 每日 00:15 UTC+8 + 手动 | `data/daily/`(日均价,滚动 1000 天)、`data/series/`、`data/table/`、`data/meta/` |

## 目录结构

```
data/
├── snapshots/YYYY-MM-DD.json   # 快照批次: { date, tz, generated, batches: [{time, items}] }
├── daily/YYYY-MM-DD.json       # (Phase 2) 日均价: avg/samples/valid/min/max/std
├── series/{slug}.json          # (Phase 2) 单物品日均价序列(折线图)
├── table/latest.json           # (Phase 2) 全物品多窗口均价/波幅/预测(表格图)
└── meta/items.json             # 物品清单(名称/中文名/类别 tags)
```

## 均价口径(与 Public-WM 一致)

- 样本 = `in-game + online` 卖单合并(`/v2/orders/item/{slug}`),offline 永不参与;
- `count>=3`:去掉最低价,取第 2 与第 3 位价格均值;`count 1~2`:全部取平均;`count=0`:`avg:null`;
- 时区:时间戳存 UTC ISO;日期键按 **UTC+8**(Asia/Shanghai,恒 +8h 无夏令时)换算。

## jsDelivr 引用

```text
https://cdn.jsdelivr.net/gh/AdminRoc/Ws-Web-price-data@main/data/meta/items.json
https://cdn.jsdelivr.net/gh/AdminRoc/Ws-Web-price-data@main/data/snapshots/2026-08-18.json
```

站点侧采用 gcore/fastly/cdn 多源 + raw 回退,取版本最新者,绝不降级旧数据。

## 本地开发

```bash
pip install aiohttp
# 冒烟测试(只抓前 5 个物品,输出到 .smoke/ 不污染 data/)
DATA_DIR=.smoke MAX_ITEMS=5 python scripts/fetch_snapshots.py
```
