import os
import io
import json
import boto3
import pandas as pd

s3 = boto3.client("s3")

S3_BUCKET = os.environ["S3_BUCKET"]
PR_CURRENT_KEY = os.environ["PR_CURRENT_KEY"]
POP_S3_KEY = os.environ["POP_S3_KEY"]

# You can also keep these from env if you want, but your CDK sets SERIES_ID/PERIOD already
SERIES_ID = os.environ.get("SERIES_ID", "PRS30006032")
PERIOD = os.environ.get("PERIOD", "Q01")


def s3_read_bytes(bucket: str, key: str) -> bytes:
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read()


def handler(event, context):
    # -----------------------
    # Load PR time series TSV from S3
    # -----------------------
    ts_bytes = s3_read_bytes(S3_BUCKET, PR_CURRENT_KEY)
    ts = pd.read_csv(io.BytesIO(ts_bytes), sep="\t", dtype=str)

    # Normalize columns + trim whitespace
    ts.columns = ts.columns.str.strip().str.lower()
    ts = ts.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Ensure required columns exist
    required_ts_cols = {"series_id", "year", "period", "value"}
    missing = required_ts_cols - set(ts.columns)
    if missing:
        raise ValueError(f"Missing columns in PR file: {sorted(list(missing))}")

    # Convert types + filter quarters
    ts["year"] = pd.to_numeric(ts["year"], errors="coerce")
    ts["value"] = pd.to_numeric(ts["value"], errors="coerce")
    ts = ts.dropna(subset=["year", "value"])
    ts["year"] = ts["year"].astype(int)

    ts = ts[ts["period"].str.match(r"^Q0[1-4]$", na=False)]

    # -----------------------
    # Load World Bank population JSON from S3
    # -----------------------
    pop_raw = s3_read_bytes(S3_BUCKET, POP_S3_KEY).decode("utf-8")
    pop_json = json.loads(pop_raw)

    # World Bank response is usually: [metadata, data]
    if isinstance(pop_json, list) and len(pop_json) > 1:
        pop_rows = pop_json[1]
    elif isinstance(pop_json, dict):
        pop_rows = pop_json.get("data", [])
    else:
        pop_rows = []

    pop = pd.DataFrame(pop_rows)

    # Normalize + pick population value
    if not pop.empty:
        pop.columns = pop.columns.str.strip().str.lower()

    if "year" not in pop.columns:
        # Sometimes the API returns "date" instead of "year"
        if "date" in pop.columns:
            pop["year"] = pop["date"]
        else:
            raise ValueError("Population JSON missing 'year'/'date' field")

    if "population" not in pop.columns:
        # World Bank uses "value" for the indicator
        if "value" in pop.columns:
            pop["population"] = pop["value"]
        else:
            raise ValueError("Population JSON missing 'population'/'value' field")

    pop["year"] = pd.to_numeric(pop["year"], errors="coerce")
    pop["population"] = pd.to_numeric(pop["population"], errors="coerce")
    pop = pop.dropna(subset=["year", "population"])
    pop["year"] = pop["year"].astype(int)

    # -----------------------
    # REPORT 1: mean/std population 2013–2018
    # -----------------------
    pop_13_18 = pop[(pop["year"] >= 2013) & (pop["year"] <= 2018)]
    mean = float(pop_13_18["population"].mean()) if not pop_13_18.empty else None
    std = float(pop_13_18["population"].std()) if not pop_13_18.empty else None
    print("REPORT 1:", mean, std)

    # -----------------------
    # REPORT 2: series with max annual sum (top 10)
    # -----------------------
    best = ts.groupby(["series_id", "year"], as_index=False)["value"].sum()
    best = best.sort_values(["value", "year"], ascending=[False, False]).drop_duplicates("series_id")
    top10 = best.head(10).to_dict("records")
    print("REPORT 2:", top10)

    # -----------------------
    # REPORT 3: join one series_id + quarter with population by year
    # -----------------------
    r3 = ts[(ts["series_id"] == SERIES_ID) & (ts["period"] == PERIOD)].merge(
        pop[["year", "population"]],
        on="year",
        how="left",
    )
    r3_preview = r3.head(10).to_dict("records")
    print("REPORT 3:", r3_preview)

    return {
        "report1": {"mean_population_2013_2018": mean, "std_population_2013_2018": std},
        "report2_top10_series": top10,
        "report3_preview": r3_preview,
    }

