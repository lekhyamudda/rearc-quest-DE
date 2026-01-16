# population_api.py
import os
import json
import hashlib
import urllib.request
import boto3

s3 = boto3.client("s3")

S3_BUCKET = os.getenv("S3_BUCKET", "rearc-bls-republish-2026")

# REQUIRED: set this in Lambda env vars
POP_API_URL = os.getenv("POP_API_URL", "")
if not POP_API_URL:
    # Don't crash at import time; raise inside function instead
    POP_API_URL = None

POP_S3_KEY = os.getenv("POP_S3_KEY", "bls/api/population.json")

USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (rearc-quest; contact: lekhyareddym14@gmail.com)"
)

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def http_get(url: str, timeout: int = 60) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()

def head_s3(bucket: str, key: str):
    try:
        return s3.head_object(Bucket=bucket, Key=key)
    except Exception:
        return None

def fetch_api_to_s3_json():
    if not POP_API_URL:
        raise ValueError("POP_API_URL environment variable is not set")

    print(f"Fetching {POP_API_URL} to {S3_BUCKET}/{POP_S3_KEY}")

    raw = http_get(POP_API_URL, timeout=60)

    # validate JSON
    data = json.loads(raw.decode("utf-8"))

    # normalize for stable hashing
    normalized = json.dumps(data, separators=(",", ":"), sort_keys=True).encode("utf-8")
    new_hash = sha256_bytes(normalized)

    meta = head_s3(S3_BUCKET, POP_S3_KEY)
    old_hash = meta.get("Metadata", {}).get("sha256") if meta else None

    if old_hash == new_hash:
        return {
            "uploaded": 0,
            "skipped_unchanged": 1,
            "bucket": S3_BUCKET,
            "key": POP_S3_KEY,
            "api_url": POP_API_URL
        }

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=POP_S3_KEY,
        Body=normalized,
        ContentType="application/json",
        Metadata={"sha256": new_hash, "source_url": POP_API_URL}
    )

    return {
        "uploaded": 1,
        "skipped_unchanged": 0,
        "bucket": S3_BUCKET,
        "key": POP_S3_KEY,
        "api_url": POP_API_URL
    }

