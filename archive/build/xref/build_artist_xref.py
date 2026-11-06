"""Build a Deezer-to-Last.fm artist cross-reference and write it to S3.

Output: s3://<bucket>/reference/artist_xref/artist_xref.jsonl, one record per Deezer artist:
  deezer_artist_id, deezer_name, lastfm_name, mbid, match_method, checked_at

match_method values:
  name_normalized   normalized name equals a Last.fm artist already collected
  lastfm_lookup     artist.getInfo resolved the name and the normalized names agree
  candidate_review  Last.fm returned a different name; stored for review but not trusted
  unmatched         Last.fm has no such artist

Inputs are the latest raw Deezer and Last.fm artist snapshots in S3. Existing records are kept
unless refresh is requested, so only new artists cost API calls.

Runs locally through main() and in Lambda through xref_handler.py.
"""
import argparse
import json
import re
import sys
import unicodedata
from datetime import datetime, timezone

import boto3
import requests
from botocore.exceptions import ClientError

from lake import XREF_KEY, to_jsonl
from lastfm_pull import LastfmError, call, get_api_key

LASTFM_ARTIST_NOT_FOUND = "code 6"


def normalize(name: str) -> str:
    """Fold case, accents, punctuation, '&' and a leading 'the' so equivalent spellings compare equal."""
    folded = unicodedata.normalize("NFKD", name)
    folded = "".join(c for c in folded if not unicodedata.combining(c))
    folded = folded.lower().replace("&", " and ")
    folded = re.sub(r"[^\w]+", " ", folded).strip()
    folded = re.sub(r"^the ", "", folded)
    # Non-Latin names (for example Korean or Japanese) must still compare by their own characters.
    return folded or name.casefold()


def read_latest_snapshot(s3, bucket: str, source: str, dataset: str) -> list[dict]:
    """Read the newest dt= partition of raw/<source>/<dataset>/ as a list of records."""
    prefix = f"raw/{source}/{dataset}/"
    partitions = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        partitions.extend(p["Prefix"] for p in page.get("CommonPrefixes", []))
    if not partitions:
        raise RuntimeError(f"No snapshots found under s3://{bucket}/{prefix}")
    latest = max(partitions)
    body = s3.get_object(Bucket=bucket, Key=f"{latest}{dataset}.jsonl")["Body"].read().decode("utf-8")
    return [json.loads(line) for line in body.splitlines() if line.strip()]


def load_lastfm_known(s3, bucket: str) -> dict[str, tuple[str, str | None]]:
    known: dict[str, tuple[str, str | None]] = {}
    for record in read_latest_snapshot(s3, bucket, "lastfm", "artists"):
        known.setdefault(normalize(record["artist_name"]), (record["artist_name"], record.get("mbid") or None))
    return known


def load_existing(s3, bucket: str) -> dict[int, dict]:
    try:
        body = s3.get_object(Bucket=bucket, Key=XREF_KEY)["Body"].read().decode("utf-8")
    except s3.exceptions.NoSuchKey:
        return {}
    records = [json.loads(line) for line in body.splitlines() if line.strip()]
    return {r["deezer_artist_id"]: r for r in records}


def resolve(http, api_key, artist_id: int, name: str, known: dict, checked_at: str) -> dict:
    base = {"deezer_artist_id": artist_id, "deezer_name": name, "checked_at": checked_at}
    hit = known.get(normalize(name))
    if hit:
        return {**base, "lastfm_name": hit[0], "mbid": hit[1], "match_method": "name_normalized"}

    try:
        info = call(http, api_key, "artist.getinfo", artist=name, autocorrect=1)["artist"]
    except LastfmError as e:
        if LASTFM_ARTIST_NOT_FOUND in str(e):
            return {**base, "lastfm_name": None, "mbid": None, "match_method": "unmatched"}
        raise
    found = info["name"]
    mbid = info.get("mbid") or None
    method = "lastfm_lookup" if normalize(found) == normalize(name) else "candidate_review"
    return {**base, "lastfm_name": found, "mbid": mbid, "match_method": method}


def refresh_xref(s3, api_key: str, bucket: str, max_new: int, refresh: bool = False) -> dict[str, int]:
    """Resolve Deezer artists that have no record yet, up to max_new, and rewrite the xref file.

    Anything beyond max_new stays unresolved and is picked up by the next run, which keeps one
    run inside the Lambda time limit.
    """
    checked_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    http = requests.Session()
    http.headers["User-Agent"] = "music-trends-pipeline/0.1 (personal portfolio project)"

    deezer = {int(r["artist_id"]): r["name"] for r in read_latest_snapshot(s3, bucket, "deezer", "artists") if r.get("name")}
    known = load_lastfm_known(s3, bucket)
    records = {} if refresh else load_existing(s3, bucket)
    todo = [(i, n) for i, n in sorted(deezer.items()) if i not in records][:max_new]
    for artist_id, name in todo:
        records[artist_id] = resolve(http, api_key, artist_id, name, known, checked_at)

    body = to_jsonl(list(records.values()), checked_at, checked_at[:10])
    s3.put_object(Bucket=bucket, Key=XREF_KEY, Body=body, ContentType="application/x-ndjson")

    counts: dict[str, int] = {}
    for record in records.values():
        counts[record["match_method"]] = counts.get(record["match_method"], 0) + 1
    counts["resolved_this_run"] = len(todo)
    counts["still_unresolved"] = len([i for i in deezer if i not in records])
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--profile", default=None)
    parser.add_argument("--region", default="us-east-2")
    parser.add_argument("--max-new", type=int, default=600, help="most new artists to resolve in one run")
    parser.add_argument("--refresh", action="store_true", help="re-resolve artists already in the file")
    args = parser.parse_args()

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    try:
        api_key = get_api_key(session)
        counts = refresh_xref(session.client("s3"), api_key, args.bucket, args.max_new, args.refresh)
    except (ClientError, RuntimeError) as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except (requests.RequestException, LastfmError) as e:
        print(f"Error from Last.fm: {e}", file=sys.stderr)
        return 1

    print(" ".join(f"{name}={n}" for name, n in sorted(counts.items())))
    print(f"Wrote s3://{args.bucket}/{XREF_KEY}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
