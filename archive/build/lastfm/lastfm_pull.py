"""Pull Last.fm tag charts, the global artist chart, and artist listener/play counts, then upload to S3.

Writes three JSON Lines datasets under raw/lastfm/<dataset>/dt=<date>/:
  tag_top_artists    one record per (tag, rank) from tag.getTopArtists
  chart_top_artists  one record per rank from chart.getTopArtists (global chart)
  artists            one record per unique artist, with listeners and playcount from artist.getInfo

The API key is read from the LASTFM_API_KEY environment variable, or, if that is not set,
from the SSM Parameter Store parameter /music-pipeline/lastfm-api-key.

Runs locally through main() and in Lambda through lastfm_handler.py.

Last.fm data is covered by its API terms: non-commercial use, caching per HTTP headers,
and attribution to Last.fm wherever the data is shown.
"""
import argparse
import os
import sys
import time
from datetime import date, datetime, timezone

import boto3
import requests
from botocore.exceptions import ClientError

from lake import (
    MIN_ROW_FRACTION,
    DataQualityError,
    check_null_share,
    check_row_count,
    load_xref_artists,
    put_dataset,
    raise_if_failed,
    to_jsonl,
)

BASE_URL = "https://ws.audioscrobbler.com/2.0/"
SSM_KEY_PARAMETER = "/music-pipeline/lastfm-api-key"
CALL_DELAY_SECONDS = 0.3
MAX_RETRIES = 5
RATE_LIMIT_ERROR_CODE = 29

# Last.fm tags are user-generated, so these are common names to start from, not a fixed taxonomy.
DEFAULT_TAGS = [
    "pop", "rock", "hip-hop", "electronic", "indie", "alternative", "rnb", "country", "jazz",
    "metal", "folk", "soul", "punk", "dance", "reggae", "blues", "classical", "latin", "k-pop",
    "funk", "house", "techno", "ambient", "hard rock", "singer-songwriter",
]


class LastfmError(Exception):
    pass


def get_api_key(session: boto3.Session) -> str:
    key = os.environ.get("LASTFM_API_KEY")
    if key:
        return key
    ssm = session.client("ssm")
    response = ssm.get_parameter(Name=SSM_KEY_PARAMETER, WithDecryption=True)
    return response["Parameter"]["Value"]


def call(http: requests.Session, api_key: str, method: str, **params) -> dict:
    """Call a Last.fm method, pacing calls and retrying rate-limit errors."""
    query = {"method": method, "api_key": api_key, "format": "json", **params}
    for attempt in range(1, MAX_RETRIES + 1):
        time.sleep(CALL_DELAY_SECONDS)
        response = http.get(BASE_URL, params=query, timeout=30)
        if response.status_code in (429, 500, 502, 503):
            time.sleep(2 * attempt)
            continue
        body = response.json()
        if "error" not in body:
            response.raise_for_status()
            return body
        if body["error"] == RATE_LIMIT_ERROR_CODE:
            time.sleep(2 * attempt)
            continue
        raise LastfmError(f"{method} {params}: {body.get('message')} (code {body['error']})")
    raise LastfmError(f"{method} {params}: gave up after {MAX_RETRIES} attempts")


def fetch_tag_artists(http, api_key, tag: str, limit: int) -> list[dict]:
    body = call(http, api_key, "tag.gettopartists", tag=tag, limit=limit)
    artists = body["topartists"]["artist"]
    return [
        {
            "tag": tag,
            "rank": int(a["@attr"]["rank"]),
            "artist_name": a["name"],
            "mbid": a.get("mbid") or None,
        }
        for a in artists
    ]


def fetch_chart(http, api_key, limit: int) -> list[dict]:
    body = call(http, api_key, "chart.gettopartists", limit=limit)
    artists = body["artists"]["artist"]
    return [
        {
            "rank": rank,
            "artist_name": a["name"],
            "mbid": a.get("mbid") or None,
            "playcount": int(a["playcount"]),
            "listeners": int(a["listeners"]),
        }
        for rank, a in enumerate(artists, start=1)
    ]


def fetch_artist(http, api_key, name: str, mbid: str | None) -> dict:
    # Prefer the MusicBrainz ID when present, since names are ambiguous.
    params = {"mbid": mbid} if mbid else {"artist": name, "autocorrect": 1}
    info = call(http, api_key, "artist.getinfo", **params)["artist"]
    stats = info.get("stats", {})
    tags = info.get("tags", {}).get("tag", [])
    return {
        "artist_name": info["name"],
        "mbid": info.get("mbid") or mbid,
        "listeners": int(stats["listeners"]) if stats.get("listeners") else None,
        "playcount": int(stats["playcount"]) if stats.get("playcount") else None,
        "top_tags": [t["name"] for t in tags],
    }


def fetch_all(
    api_key: str,
    tags: list[str],
    per_tag: int,
    chart_size: int,
    extra_artists: list[tuple[str, str | None]] | None = None,
) -> dict[str, list[dict]]:
    """Fetch every dataset from Last.fm. Raises on any failure so no partial day is uploaded.

    extra_artists are (name, mbid) pairs, such as artists matched from Deezer, that also get
    an artist.getInfo lookup so their listener counts are tracked daily.
    """
    http = requests.Session()
    http.headers["User-Agent"] = "music-trends-pipeline/0.1 (personal portfolio project)"

    tag_rows: list[dict] = []
    for tag in tags:
        tag_rows.extend(fetch_tag_artists(http, api_key, tag, per_tag))
    chart_rows = fetch_chart(http, api_key, chart_size)

    # One lookup per unique artist, keyed by MusicBrainz ID when available, else by name.
    unique: dict[str, tuple[str, str | None]] = {}
    for row in tag_rows + chart_rows:
        unique.setdefault(row["mbid"] or row["artist_name"].lower(), (row["artist_name"], row["mbid"]))
    for name, mbid in extra_artists or []:
        unique.setdefault(mbid or name.lower(), (name, mbid))
    artists = [fetch_artist(http, api_key, name, mbid) for name, mbid in unique.values()]

    return {"tag_top_artists": tag_rows, "chart_top_artists": chart_rows, "artists": artists}


def validate(
    datasets: dict[str, list[dict]], tags: list[str], per_tag: int, chart_size: int, extra_count: int
) -> list[str]:
    """Return a list of problems with a fetched snapshot; empty means it looks complete."""
    tag_rows = datasets["tag_top_artists"]
    chart_rows = datasets["chart_top_artists"]
    artists = datasets["artists"]

    problems: list[str] = []
    problems += check_row_count("tag_top_artists", len(tag_rows), len(tags) * per_tag)
    problems += check_row_count("chart_top_artists", len(chart_rows), chart_size)

    returned_tags = {row["tag"] for row in tag_rows}
    empty = [tag for tag in tags if tag not in returned_tags]
    if empty:
        problems.append(f"tags with no artists: {empty}")

    # Every cross-referenced artist is looked up daily, so the artists table can not be much smaller.
    if len(artists) < extra_count * MIN_ROW_FRACTION:
        problems.append(f"artists: {len(artists)} rows, expected at least {extra_count} cross-referenced artists")
    problems += check_null_share("artists.listeners", sum(1 for a in artists if a["listeners"] is None), len(artists))
    return problems


def pull_and_upload(
    s3, api_key: str, bucket: str, load_date: str, tags: list[str], per_tag: int, chart_size: int
) -> dict[str, int]:
    """Fetch all datasets, check them, then write them to S3. Returns row counts per dataset.

    A failed check raises before anything is written, so a bad run never overwrites a good day.
    """
    fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    extra_artists = load_xref_artists(s3, bucket)
    datasets = fetch_all(api_key, tags, per_tag, chart_size, extra_artists)
    raise_if_failed(validate(datasets, tags, per_tag, chart_size, len(extra_artists)))
    for dataset, records in datasets.items():
        body = to_jsonl(records, fetched_at, load_date)
        key = put_dataset(s3, bucket, "lastfm", dataset, load_date, body)
        print(f"Uploaded s3://{bucket}/{key}")
    return {dataset: len(records) for dataset, records in datasets.items()}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--profile", default=None)
    parser.add_argument("--region", default="us-east-2")
    parser.add_argument("--tags", nargs="+", default=DEFAULT_TAGS, help="Last.fm tags to chart")
    parser.add_argument("--per-tag", type=int, default=25, help="artists per tag")
    parser.add_argument("--chart-size", type=int, default=100, help="size of the global artist chart")
    parser.add_argument("--date", default=date.today().isoformat(), help="load date, default today")
    parser.add_argument("--dry-run", action="store_true", help="fetch and print counts, do not upload")
    args = parser.parse_args()

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    try:
        api_key = get_api_key(session)
        if args.dry_run:
            datasets = fetch_all(api_key, args.tags, args.per_tag, args.chart_size)
            raise_if_failed(validate(datasets, args.tags, args.per_tag, args.chart_size, 0))
            counts = {name: len(rows) for name, rows in datasets.items()}
        else:
            counts = pull_and_upload(
                session.client("s3"), api_key, args.bucket, args.date, args.tags, args.per_tag, args.chart_size
            )
    except ClientError as e:
        print(f"Error talking to AWS: {e}", file=sys.stderr)
        return 1
    except (requests.RequestException, LastfmError) as e:
        print(f"Error fetching from Last.fm: {e}", file=sys.stderr)
        return 1
    except DataQualityError as e:
        print(f"Data quality check failed, nothing uploaded: {e}", file=sys.stderr)
        return 1

    print(" ".join(f"{name}={count}" for name, count in counts.items()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
