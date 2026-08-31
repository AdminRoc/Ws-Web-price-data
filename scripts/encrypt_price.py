#!/usr/bin/env python3
"""
encrypt_price.py — 产仓落盘前最后一刻加密（仅公库暴露内容，Ws-Web 白名单跳过）

用法：
  python3 .github/scripts/encrypt_price.py --all          # 按仓库自动识别清单
  python3 .github/scripts/encrypt_price.py --decrypt-all  # 回滚：密文→明文
  python3 .github/scripts/encrypt_price.py data/avg_prices_full.json

清单（仅 *.json 文字库，二进制/榜单排除）：
  Ws-Web-price-data: data/snapshots/*.json data/daily/*.json data/series/*.json data/table/latest.json data/meta/*.json
  Public-WM:         data/avg_prices_full.json [+ wm-items/auction-dicts 需评估 KV 限额与下游依赖，默认仅 avg_prices]
  Ws-Web-relic-data: data/prices.json data/prices-summary.json data/relics.json data/reward-items.json data/item-categories.json data/item-names-zh.json data/relic-deep-date*.json
  Ws-Web-assets:     data/item/icon-index.json data/manifest.json data/item/*.json data/i18n/*.json (drops-index 超限仅 cdn，不进 KV 但仍加密)
  Ws-Web (榜单):     skip（白名单）

历史泄漏边界：加密前已 push 的 git log 明文与旧 cdn SHA 无法追溯擦除，仅新 commit 为密文。
"""
import argparse
import glob
import json
import os
import sys

import crypto_price

# 仅公库暴露需反爬；私库（cdn 404）不产密文
PUBLIC_PRODUCERS = {"Ws-Web-price-data", "Public-WM", "Ws-Web-relic-data", "Ws-Web-assets"}

# 白名单：永不加密
SKIP_REPOS = {"Ws-Web"}

# 二进制/非文字库排除
SKIP_EXTS = {".png", ".jpg", ".jpeg", ".webp", ".gif", ".ico", ".woff", ".woff2", ".ttf", ".bmp"}


def repo_name() -> str:
    # 通过 git remote 或环境变量识别
    env = os.environ.get("GITHUB_REPOSITORY", "")
    if env:
        return env.split("/")[-1]
    try:
        import subprocess
        out = subprocess.check_output(["git", "remote", "get-url", "origin"], text=True, timeout=5)
        return out.strip().split("/")[-1].replace(".git", "")
    except Exception:
        return ""


def should_encrypt(path: str) -> bool:
    if os.path.splitext(path)[1].lower() in SKIP_EXTS:
        return False
    # 仅 *.json 文字库
    if not path.endswith(".json"):
        return False
    return True


def encrypt_file(path: str, decrypt_first: bool = False) -> bool:
    """加密单个文件（若已是密文则先解密再重加密以防重复包装）；返回是否写入"""
    if not os.path.exists(path):
        return False
    if not should_encrypt(path):
        print(f"  skip (non-json): {path}")
        return False
    # 读取（透传解密得明文对象）
    obj = crypto_price.load_json(path)
    if obj is None:
        print(f"  skip (load fail): {path}")
        return False
    # 若原文件已是密文，load_json 已得明文；直接重加密即可（避免 ct 套 ct）
    raw = crypto_price.load_raw(path)
    is_wrapped = isinstance(raw, dict) and raw.get("ct") and raw.get("iv")
    # 校验：若声称已加密但 load_json 失败，说明密钥不匹配，阻断
    if is_wrapped and obj is None:
        print(f"  ERROR: decrypt failed (key mismatch): {path}")
        return False
    crypto_price.save_json_encrypt(path, obj)
    print(f"  encrypted: {path} {'(re-encrypted)' if is_wrapped else ''}")
    return True


def decrypt_file(path: str) -> bool:
    """回滚：密文→明文"""
    raw = crypto_price.load_raw(path)
    if not (isinstance(raw, dict) and raw.get("ct") and raw.get("iv")):
        print(f"  skip (not encrypted): {path}")
        return False
    try:
        obj = crypto_price.load_json(path)  # 已解密
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
        print(f"  decrypted: {path}")
        return True
    except Exception as e:
        print(f"  ERROR decrypt {path}: {e}")
        return False


def collect_all() -> list:
    rn = repo_name()
    if rn in SKIP_REPOS:
        print(f"skip repo (whitelist): {rn}")
        return []
    # 按仓库识别清单
    if rn == "Ws-Web-price-data":
        patterns = [
            "data/snapshots/*.json",
            "data/daily/*.json",
            "data/series/*.json",
            "data/table/*.json",
            "data/table/*.json",
            "data/meta/*.json",
        ]
    elif rn == "Public-WM":
        patterns = ["data/avg_prices_full.json"]
        # 可选扩面：wm-items/auction-dicts 同 SECRET 但需评估 KV 限额与下游，此处默认不自动加密
        # patterns += ["data/wm-items.json", "data/auction-dicts.json"]
    elif rn == "Ws-Web-relic-data":
        patterns = [
            "data/prices.json",
            "data/prices-summary.json",
            "data/relics.json",
            "data/reward-items.json",
            "data/item-categories.json",
            "data/item-names-zh.json",
            "data/relic-deep-date.json",
            "data/relic-deep-date-summary.json",
            "data/update-versions.json",
        ]
    elif rn == "Ws-Web-assets":
        patterns = [
            "data/item/*.json",
            "data/i18n/*.json",
            "data/worldstate/*.json",
            "data/tenet-coda-rotation.json",
            "data/arbitration-metrics/*.json",
            "manifest.json",
        ]
    else:
        # 未知仓库：兜底仅 data/**/*.json（Ws-Web 会在此前已 skip）
        patterns = ["data/**/*.json"]
    files = []
    for pat in patterns:
        files.extend(glob.glob(pat, recursive=True))
    # 去重且仅保留存在且可加密的
    files = sorted(set(f for f in files if os.path.exists(f) and should_encrypt(f)))
    return files


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--all", action="store_true", help="按仓库自动清单加密")
    ap.add_argument("--decrypt-all", action="store_true", help="回滚：密文→明文")
    ap.add_argument("files", nargs="*", help="指定文件加密")
    args = ap.parse_args()

    if args.decrypt_all:
        files = collect_all() if not args.files else args.files
        if not files:
            files = glob.glob("data/**/*.json", recursive=True)
        n = sum(1 for f in files if decrypt_file(f))
        print(f"\nDone: decrypted {n}/{len(files)}")
        return 0

    if args.all:
        files = collect_all()
        if not files:
            print("no files to encrypt")
            return 0
        print(f"repo={repo_name()}  files={len(files)}")
        n = sum(1 for f in files if encrypt_file(f))
        print(f"\nDone: encrypted {n}/{len(files)}")
        # 校验：回环 decrypt(encrypt) == plain
        for f in files[:5]:
            try:
                obj = crypto_price.load_json(f)
                assert obj is not None, f"verify load fail {f}"
            except Exception as e:
                print(f"VERIFY FAIL {f}: {e}")
                return 1
        return 0

    if args.files:
        n = sum(1 for f in args.files if encrypt_file(f))
        print(f"\nDone: encrypted {n}/{len(args.files)}")
        return 0

    ap.print_help()
    return 1


if __name__ == "__main__":
    # Ws-Web 白名单硬阻断
    if repo_name() in SKIP_REPOS:
        print(f"skip encrypt (whitelist repo={repo_name()})")
        sys.exit(0)
    # 私库不暴露无需加密：若仓库为私库（cdn 404）但被误调 --all，仍按清单执行但仅公库清单有效；私库消仓无需产密文
    # 此处不额外阻断，依赖 collect_all 的仓库识别
    if not os.environ.get("PRICE_DATA_SECRET"):
        print("ERROR: PRICE_DATA_SECRET not set", file=sys.stderr)
        sys.exit(1)
    sys.exit(main())
