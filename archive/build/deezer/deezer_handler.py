"""Lambda entry point for the daily Deezer pull."""
import os
from datetime import datetime, timezone

import boto3

from deezer_pull import pull_and_upload

s3 = boto3.client("s3")


def lambda_handler(event, context):
    bucket = os.environ["DATA_BUCKET"]
    per_genre = int(os.environ.get("PER_GENRE", "25"))
    # An explicit load_date in the event allows a manual re-run for a given day.
    load_date = (event or {}).get("load_date") or datetime.now(timezone.utc).date().isoformat()
    counts = pull_and_upload(s3, bucket, load_date, per_genre)
    return {"load_date": load_date, "rows": counts}
