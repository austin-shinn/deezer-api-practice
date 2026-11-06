"""Lambda entry point for the daily Last.fm pull."""
import os
from datetime import datetime, timezone

import boto3

from lastfm_pull import DEFAULT_TAGS, get_api_key, pull_and_upload

session = boto3.Session()
s3 = session.client("s3")


def lambda_handler(event, context):
    bucket = os.environ["DATA_BUCKET"]
    per_tag = int(os.environ.get("PER_TAG", "25"))
    chart_size = int(os.environ.get("CHART_SIZE", "100"))
    # An explicit load_date in the event allows a manual re-run for a given day.
    load_date = (event or {}).get("load_date") or datetime.now(timezone.utc).date().isoformat()
    api_key = get_api_key(session)
    counts = pull_and_upload(s3, api_key, bucket, load_date, DEFAULT_TAGS, per_tag, chart_size)
    return {"load_date": load_date, "rows": counts}
