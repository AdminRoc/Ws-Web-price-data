#!/usr/bin/env python3
"""
crypto_price.py — 均价及文字库统一加解密真源（Ws-Web-core 私库分发）

- 仅公库暴露内容反爬，私库不落密文（仅消费侧解密）
- 算法：AES-256-GCM + HKDF-SHA256(salt="wfspeed-price-v1", info="price-data-enc")
- 包装：{"v":1,"alg":"AES-GCM","iv":"base64(12B)","ct":"base64(ciphertext+tag)","sha256":"hex(SHA256(plain))"}
- 路径不变（同名覆写），仅 *.json 文字库，二进制/榜单排除，Ws-Web 白名单 skip
- 多日累计零失真：load_json() 自动识别 ct/iv 包装，无包装透传明文（灰度），跨日先解密
- 历史泄漏边界：加密前已 push 的 git log 明文与旧 cdn SHA 无法追溯擦除

被产仓以 `cp -a _private/.github/scripts/. .github/scripts/` 覆盖执行。
"""
import base64
import hashlib
import json
import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives import hashes


def _derive_key(secret: str, info: bytes = b"price-data-enc") -> bytes:
    hkdf = HKDF(algorithm=hashes.SHA256(), length=32, salt=b"wfspeed-price-v1", info=info)
    return hkdf.derive(secret.encode("utf-8"))


def _get_key() -> bytes:
    secret = os.environ.get("PRICE_DATA_SECRET")
    if not secret:
        raise RuntimeError("PRICE_DATA_SECRET not set")
    return _derive_key(secret, b"price-data-enc")


def encrypt_bytes(plain: bytes) -> dict:
    key = _get_key()
    iv = os.urandom(12)
    ct = AESGCM(key).encrypt(iv, plain, None)
    return {
        "v": 1,
        "alg": "AES-GCM",
        "iv": base64.b64encode(iv).decode("ascii"),
        "ct": base64.b64encode(ct).decode("ascii"),
        "sha256": hashlib.sha256(plain).hexdigest(),
    }


def decrypt_obj(obj: dict) -> bytes:
    key = _get_key()
    iv = base64.b64decode(obj["iv"])
    ct = base64.b64decode(obj["ct"])
    plain = AESGCM(key).decrypt(iv, ct, None)
    if hashlib.sha256(plain).hexdigest() != obj.get("sha256"):
        raise ValueError("sha256 mismatch")
    return plain


def load_json(path: str):
    """透传解密：若文件为包装密文则解密后返回对象，否则按明文返回；失败返回 None"""
    try:
        raw = open(path, "r", encoding="utf-8").read()
        j = json.loads(raw)
        if isinstance(j, dict) and j.get("ct") and j.get("iv"):
            return json.loads(decrypt_obj(j).decode("utf-8"))
        return j
    except Exception:
        return None


def load_raw(path: str):
    """返回原始 JSON 对象（不解密），用于判断是否为包装"""
    try:
        return json.loads(open(path, "r", encoding="utf-8").read())
    except Exception:
        return None


def save_json_encrypt(path: str, obj) -> None:
    """明文对象 → 确定性 JSON → 加密包装覆写原路径"""
    plain = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    wrapper = encrypt_bytes(plain)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(wrapper, f, ensure_ascii=False)


def is_encrypted(path: str) -> bool:
    j = load_raw(path)
    return isinstance(j, dict) and bool(j.get("ct") and j.get("iv"))


# —— JS 侧 HKDF 对应（WebCrypto subtle）——
# const keyRaw = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(secret)) // 简化版
# 实际应 HKDF：await crypto.subtle.deriveBits({name:'HKDF', hash:'SHA-256', salt:..., info:...}, baseKey, 256)
# 为保持 Python/JS 同参，JS 侧实现见 docs/PRICE_ENCRYPTION_PLAN.md §5.1
