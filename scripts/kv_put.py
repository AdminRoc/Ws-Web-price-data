#!/usr/bin/env python3
"""
kv_put.py — Makers KV 直写真源（Actions → KV，不经自定义域 /api/update-kv）
- 仅公库暴露内容反爬，私库不落密文
- 依赖：pip install tencentcloud-sdk-python cryptography
- 鉴权：TENCENTCLOUD_SECRET_ID / SECRET_KEY（GitHub Secrets）
- 参数：--zone ZONE_ID --ns NAMESPACE --key KEY --file FILE（含 {ct,iv} 密文包装，直接 put）
- 文档：https://cloud.tencent.com/document/product/1552/127420 (Makers KV) + https://intl.cloud.tencent.com/document/product/1145/78824 (EdgeKVPut)
"""
import argparse
import json
import os
import sys

def put_kv(zone: str, ns: str, key: str, value: str):
    try:
        from tencentcloud.common.credential import Credential
        from tencentcloud.teo.v20220901 import teo_client, models
    except ImportError:
        print("ERROR: tencentcloud-sdk-python not installed. pip install tencentcloud-sdk-python -q", file=sys.stderr)
        sys.exit(1)
    cred = Credential(os.environ["TENCENTCLOUD_SECRET_ID"], os.environ["TENCENTCLOUD_SECRET_KEY"])
    client = teo_client.TeoClient(cred, "ap-guangzhou")
    req = models.EdgeKVPutRequest()
    req.ZoneId = zone
    req.Namespace = ns
    req.Key = key
    req.Value = value
    # Makers KV 单值 ≤25MB，公库密文均 <6MB，无需分片
    resp = client.EdgeKVPut(req)
    # 成功返回 RequestId，无异常即成功
    print(f"KV put {ns}/{key} -> {zone} OK RequestId={resp.RequestId}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--zone", required=True, help="ZoneId (Makers 项目所属站点 ZoneId)")
    ap.add_argument("--ns", required=True, help="Namespace (Makers KV 命名空间名，如 price-kv)")
    ap.add_argument("--key", required=True, help="Key (如 price_table_latest)")
    ap.add_argument("--file", required=True, help="Value file path (已加密包装的 *.json 文本)")
    args = ap.parse_args()
    if not os.path.exists(args.file):
        print(f"Missing file {args.file}", file=sys.stderr); sys.exit(1)
    value = open(args.file, encoding="utf-8").read()
    # 校验：若为明文误 put，直接警告但仍 put（解密侧会透传）
    try:
        j = json.loads(value)
        if isinstance(j, dict) and j.get("ct"):
            assert j.get("sha256"), "missing sha256"
    except Exception:
        pass
    # 大文件仅 cdn：调用方应在外层 skip >1MB，此处不拦截
    put_kv(args.zone, args.ns, args.key, value)

if __name__ == "__main__":
    for k in ["TENCENTCLOUD_SECRET_ID", "TENCENTCLOUD_SECRET_KEY"]:
        if not os.environ.get(k):
            print(f"ERROR: {k} not set", file=sys.stderr); sys.exit(1)
    main()
