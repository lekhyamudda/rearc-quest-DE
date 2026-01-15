import os
import json
import urllib.request
import hashlib
import boto3

s3 = boto3.client("s3")

# Read environment variables
BUCKET = os.environ["S3_BUCKET"]

POP_KEY = "bls/api/population.json"
TS_KEY  = "bls/pr/pr.data.0.Current"

POP_URL = "https://api.worldbank.org/v2/country/USA/indicator/SP.POP.TOTL?format=json"
TS_URL  = "https://download.bls.gov/pub/time.series/pr/pr.data.0.Current"

HEADERS = {
    "User-Agent": "rearc-quest (contact: lekhyareddym14@gmail.com)"
}

def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def fetch(url: str) -> bytes:
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req) as r:
        return r.read()

def handler(event, context):
    pop_data = fetch(POP_URL)
    s3.put_object(Bucket=BUCKET, Key=POP_KEY, Body=pop_data)

    ts_data = fetch(TS_URL)

    try:
        old = s3.get_object(Bucket=BUCKET, Key=TS_KEY)["Body"].read()
        if sha256(old) == sha256(ts_data):
            return {"status": "no-change"}
    except Exception:
        pass

    s3.put_object(Bucket=BUCKET, Key=TS_KEY, Body=ts_data)
    return {"status": "updated"}

