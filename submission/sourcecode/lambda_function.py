# lambda_function.py
from bls_sync import sync_bls_to_s3
from population_api import fetch_api_to_s3_json

def lambda_handler(event, context):
    part1 = sync_bls_to_s3()
    part2 = fetch_api_to_s3_json()
    return {"part1": part1, "part2": part2}


