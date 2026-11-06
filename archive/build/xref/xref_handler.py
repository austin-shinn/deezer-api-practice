"""Lambda entry point for the weekly Deezer-to-Last.fm artist cross-reference refresh."""
import os

import boto3

from build_artist_xref import refresh_xref
from lastfm_pull import get_api_key

session = boto3.Session()
s3 = session.client("s3")


def lambda_handler(event, context):
    bucket = os.environ["DATA_BUCKET"]
    max_new = int(os.environ.get("MAX_NEW", "600"))
    api_key = get_api_key(session)
    return refresh_xref(s3, api_key, bucket, max_new)
