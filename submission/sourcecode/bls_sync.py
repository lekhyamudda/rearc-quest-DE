# bls_sync.py
import os, re, hashlib, urllib.request
from urllib.parse import urljoin
import boto3

BASE_URL   = os.getenv("BASE_URL", "https://download.bls.gov/pub/time.series/pr/")
S3_BUCKET  = os.getenv("S3_BUCKET", "rearc-bls-republish-2026")
S3_PREFIX  = os.getenv("S3_PREFIX", "bls/pr/")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
USER_AGENT = os.getenv("USER_AGENT", "Mozilla/5.0 (rearc-quest; contact: lekhyareddym14@gmail.com)")
MAX_FILES  = int(os.getenv("MAX_FILES", "0"))

s3 = boto3.client("s3", region_name=AWS_REGION)

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def http_get(url: str, timeout: int = 60) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()

def list_remote_files() -> list[str]:
    html = http_get(BASE_URL, timeout=30).decode("utf-8", errors="ignore")
    pattern = r"""<a\s+href\s*=\s*(?:"([^"]+)"|'([^']+)'|([^\s>]+))"""
    matches = re.findall(pattern, html, flags=re.IGNORECASE)

    links = []
    for g1, g2, g3 in matches:
        link = g1 or g2 or g3
        if link:
            links.append(link.strip())

    files = []
    for link in links:
        if "/pub/time.series/pr/" in link:
            filename = link.split("/pub/time.series/pr/")[-1]
        else:
            filename = link
        filename = filename.strip()

        if not filename or filename.endswith("/"):
            continue
        if filename in ("../", "./", "index.html"):
            continue
        if filename.startswith("?") or filename.startswith("#") or filename.startswith("/"):
            continue
        if "/" in filename:
            continue
        files.append(filename)

    files = sorted(set(files))
    if "pr.data.0.Current" in files:
        files.remove("pr.data.0.Current")
        files.insert(0, "pr.data.0.Current")

    if MAX_FILES and len(files) > MAX_FILES:
        files = files[:MAX_FILES]
    return files

def list_s3_keys(bucket: str, prefix: str) -> set[str]:
    keys = set()
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token

        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            keys.add(obj["Key"])

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return keys

def head_s3(bucket: str, key: str):
    try:
        return s3.head_object(Bucket=bucket, Key=key)
    except Exception:
        return None

def sync_bls_to_s3():
    remote_files = list_remote_files()
    remote_keys = {f"{S3_PREFIX}{name}" for name in remote_files}
    existing_keys = list_s3_keys(S3_BUCKET, S3_PREFIX)

    uploaded = skipped = deleted = failed = 0

    for name in remote_files:
        try:
            file_url = urljoin(BASE_URL, name)
            key = f"{S3_PREFIX}{name}"

            data = http_get(file_url, timeout=60)
            new_hash = sha256_bytes(data)

            meta = head_s3(S3_BUCKET, key)
            old_hash = meta.get("Metadata", {}).get("sha256") if meta else None

            if old_hash == new_hash:
                skipped += 1
                continue

            s3.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=data,
                ContentType="text/plain",
                Metadata={"sha256": new_hash, "source_url": file_url}
            )
            uploaded += 1
        except Exception as e:
            print(f"FAILED upload {name}: {e}")
            failed += 1

    to_delete = sorted(list(existing_keys - remote_keys))
    to_delete = [k for k in to_delete if k.startswith(S3_PREFIX) and not k.endswith("/")]

    for key in to_delete:
        try:
            s3.delete_object(Bucket=S3_BUCKET, Key=key)
            deleted += 1
        except Exception as e:
            print(f"FAILED delete {key}: {e}")
            failed += 1

    result = {
        "bucket": S3_BUCKET,
        "prefix": S3_PREFIX,
        "remote_count": len(remote_files),
        "existing_count": len(existing_keys),
        "uploaded": uploaded,
        "skipped_unchanged": skipped,
        "deleted_missing_from_source": deleted,
        "failed": failed
    }
    print("RESULT:", result)
    return result

