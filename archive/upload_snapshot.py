"""Upload a local file to S3 under a date-partitioned raw key.

Key layout: raw/<source>/<dataset>/dt=YYYY-MM-DD/<filename>
Re-running on the same day overwrites that day's object instead of duplicating it.
"""
import argparse
import sys
from datetime import date
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from lake import build_key

CONTENT_TYPES = {".json": "application/json", ".parquet": "application/octet-stream"}


def upload(session: boto3.Session, bucket: str, path: Path, key: str) -> None:
    s3 = session.client("s3")
    extra = {"ContentType": CONTENT_TYPES.get(path.suffix, "binary/octet-stream")}
    s3.upload_file(str(path), bucket, key, ExtraArgs=extra)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file", type=Path)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--source", required=True, help="e.g. deezer, lastfm, spotify")
    parser.add_argument("--dataset", required=True, help="e.g. genres, artists")
    parser.add_argument("--date", default=date.today().isoformat(), help="load date, default today")
    parser.add_argument("--profile", default=None)
    parser.add_argument("--region", default="us-east-2")
    args = parser.parse_args()

    if not args.file.is_file():
        print(f"Error: {args.file} is not a file", file=sys.stderr)
        return 1

    key = build_key(args.source, args.dataset, args.date, args.file.name)
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    try:
        upload(session, args.bucket, args.file, key)
    except ClientError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    print(f"Uploaded to s3://{args.bucket}/{key}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
