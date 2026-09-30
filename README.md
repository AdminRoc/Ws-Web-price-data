# Ws-Web-price-data

**Ws-Web-price 的公开数据仓** —— 全部数据 action 在此运作(公库 action 分钟数不限),产物提交进 `data/`,经 **jsDelivr 反向引用**到站点 `Ws-Web-price`(`price.wfspeed.run`),与 relic 模块(`Ws-Web-relic-data` ↔ `Ws-Web-relic`)同构。

## 数据管线

| 工作流 | 触发 | 产物 |
|---|---|---|
| `fetch-snapshots.yml` | 每 2h(UTC 第 15 分)主计划、每小时第 45 分 stale-only 兜底 + 手动 | 快照批次 + **日均价聚合(幂等)** + 表格 bundle + meta,一次提交完成 |
| — | 聚合语义 | 目标日 = 前一日(UTC+8);`data/daily/`(日均价,滚动 1000 天)、`data/series/`、`data/table/`、`data/meta/` |

快照工作流遇到远端推进时，只对本次生成的数据提交执行 fetch + rebase；如果同一数据文件存在无法自动合并的冲突，就停止并且不运行 jsDelivr/PRICE_KV 发布步骤。不得通过重置远端分支并整体覆盖 `data/` 来“恢复”快照。Warframe.market 请求与限速逻辑不属于此冲突处理。

定时运行在读取上一次 workflow 的完整成功状态(包括 `PRICE_KV` 读回)后才按 90 分钟新鲜度门槛决定是否抓取；近时重复的主计划/兜底计划会跳过，手动 `workflow_dispatch` 不受门槛限制。GitHub API 状态不可核实时，定时门控失败关闭，不发起 Warframe.market 请求，也不写数据。

`PRICE_KV` 当日快照采用有界分片发布：每个批次按物品键拆成不超过 768 KiB 的内容寻址分片，工作流逐片写入并校验哈希/字节数，再发布 `price_today_snapshots_manifest`；`price_meta` 始终最后写入作为就绪标记。公开源 `data/snapshots/YYYY-MM-DD.json` 保持完整、不拆改历史。消费端只接受 generation、revision、日期、批次数、末次时间和所有分片完整性均与就绪元数据匹配的数据集。KV 侧与源快照相同保留 7 天：每次写入前只清理该专用前缀下早于安全 cutoff 的分片，并核算留存值及本次增量；当快照分片预算达到 128 MiB 时在任何新数据键写入前失败关闭，不继续扩大存储。

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

- 先从 `/v2/items` 取得稳定 `id`，再用 `/v2/orders/itemId/{id}` 抓订单；数据表仍以当前 slug 作为输出键。样本 = `in-game + online` 卖单合并,offline 永不参与;
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
