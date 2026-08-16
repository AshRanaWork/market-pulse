"""Export the S3 raw zone (plus dbt seeds) into one zip for upload to a
Databricks Unity Catalog Volume.

Databricks Free Edition runs on serverless compute, which does not accept
custom S3 credentials, so the portable path is: pull the raw files down
with your local AWS credentials, zip them, upload the single zip through
the Volumes UI. Notebook 01 unpacks it on the Databricks side.

Usage:
    export AWS_ACCESS_KEY_ID=...      (or rely on your default profile)
    export AWS_SECRET_ACCESS_KEY=...
    python3 spark/export_raw_for_databricks.py market-pulse-ashranawork
"""

import io
import sys
import zipfile
from pathlib import Path

import boto3


def main(bucket: str) -> None:
    s3 = boto3.client("s3")
    out = Path("databricks_upload.zip")
    paginator = s3.get_paginator("list_objects_v2")

    n = 0
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zf:
        for page in paginator.paginate(Bucket=bucket, Prefix="raw/"):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".csv"):
                    continue
                body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
                zf.writestr(key, body)
                n += 1
        for seed in ("events.csv", "holidays.csv"):
            p = Path("dbt_project/seeds") / seed
            zf.writestr(f"seeds/{seed}", p.read_text())

    print(f"Wrote {out} with {n} raw files + 2 seeds "
          f"({out.stat().st_size/1024:.0f} KB)")
    print("Upload this single zip to your Databricks Volume, then run "
          "notebook 01.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: python3 spark/export_raw_for_databricks.py <bucket>")
    main(sys.argv[1])
