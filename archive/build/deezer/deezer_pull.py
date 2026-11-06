"""Pull Deezer genres, per-genre track charts, and artist fan counts, then upload to S3.

Writes three JSON Lines datasets under raw/deezer/<dataset>/dt=<date>/:
  genres              one record per Deezer genre
  genre_chart_tracks  one record per (genre, position) from /chart/<genre>/tracks
  artists             one record per unique charted artist, with nb_fan and nb_album

Public Deezer endpoints need no key. Deezer reports a limit of about 50 calls per
5 seconds, so calls are paced and quota errors are retried.

Runs locally through main() and in Lambda through deezer_handler.py.
"""
import argparse
import sys
import time
from datetime import date, datetime, timezone

import boto3
import requests
from botocore.exceptions import ClientError

from lake import (
    DataQualityError,
    check_null_share,
    check_row_count,
    put_dataset,
    raise_if_failed,
    to_jsonl,
)

BASE_URL = "https://api.deezer.com"
MIN_GENRES = 20
CALL_DELAY_SECONDS = 0.25
MAX_RETRIES = 5
QUOTA_ERROR_CODE = 4


class DeezerError(Exception):
    pass


def get_json(session: requests.Session, path: str, params: dict | None = None) -> dict:
    """GET a Deezer endpoint, pacing calls and retrying quota errors."""
    for attempt in range(1, MAX_RETRIES + 1):
        time.sleep(CALL_DELAY_SECONDS)
        response = session.get(f"{BASE_URL}{path}", params=params, timeout=30)
        if response.status_code == 429:
            time.sleep(2 * attempt)
            continue
        response.raise_for_status()
        body = response.json()
        error = body.get("error") if isinstance(body, dict) else None
        if error is None:
            return body
        if error.get("code") == QUOTA_ERROR_CODE:
            time.sleep(2 * attempt)
            continue
        raise DeezerError(f"{path}: {error}")
    raise DeezerError(f"{path}: gave up after {MAX_RETRIES} attempts")


def fetch_genres(session: requests.Session) -> list[dict]:
    genres = get_json(session, "/genre")["data"]
    return [g for g in genres if g["id"] != 0]


def fetch_genre_chart(session: requests.Session, genre: dict, per_genre: int) -> list[dict]:
    # The artist endpoints (/chart/<id>/artists, /genre/<id>/artists) ignore the genre and
    # return the overall chart, so genre membership is derived from the genre track chart.
    tracks = get_json(session, f"/chart/{genre['id']}/tracks", {"limit": per_genre})["data"]
    return [
        {
            "genre_id": genre["id"],
            "genre_name": genre["name"],
            "position": t["position"],
            "track_id": t["id"],
            "track_title": t["title"],
            "track_rank": t["rank"],
            "artist_id": t["artist"]["id"],
            "artist_name": t["artist"]["name"],
        }
        for t in tracks
    ]


def fetch_artist(session: requests.Session, artist_id: int) -> dict:
    a = get_json(session, f"/artist/{artist_id}")
    return {
        "artist_id": a["id"],
        "name": a["name"],
        "nb_fan": a.get("nb_fan"),
        "nb_album": a.get("nb_album"),
    }


def fetch_all(per_genre: int) -> dict[str, list[dict]]:
    """Fetch every dataset from Deezer. Raises on any failure so no partial day is uploaded."""
    http = requests.Session()
    http.headers["User-Agent"] = "music-trends-pipeline/0.1 (personal portfolio project)"

    genres = fetch_genres(http)
    chart_rows: list[dict] = []
    for genre in genres:
        chart_rows.extend(fetch_genre_chart(http, genre, per_genre))
    artist_ids = sorted({row["artist_id"] for row in chart_rows})
    artists = [fetch_artist(http, artist_id) for artist_id in artist_ids]

    return {
        "genres": [{"genre_id": g["id"], "genre_name": g["name"]} for g in genres],
        "genre_chart_tracks": chart_rows,
        "artists": artists,
    }


def validate(datasets: dict[str, list[dict]], per_genre: int) -> list[str]:
    """Return a list of problems with a fetched snapshot; empty means it looks complete."""
    genres = datasets["genres"]
    chart = datasets["genre_chart_tracks"]
    artists = datasets["artists"]

    problems: list[str] = []
    if len(genres) < MIN_GENRES:
        problems.append(f"genres: {len(genres)} rows, expected at least {MIN_GENRES}")
    problems += check_row_count("genre_chart_tracks", len(chart), len(genres) * per_genre)

    charted_genres = {row["genre_id"] for row in chart}
    empty = [g["genre_name"] for g in genres if g["genre_id"] not in charted_genres]
    if empty:
        problems.append(f"genres with no chart rows: {empty}")

    charted_artists = {row["artist_id"] for row in chart}
    if len(artists) != len(charted_artists):
        problems.append(f"artists: {len(artists)} rows for {len(charted_artists)} charted artists")
    problems += check_null_share("artists.nb_fan", sum(1 for a in artists if a["nb_fan"] is None), len(artists))
    return problems


def pull_and_upload(s3, bucket: str, load_date: str, per_genre: int) -> dict[str, int]:
    """Fetch all datasets, check them, then write them to S3. Returns row counts per dataset.

    A failed check raises before anything is written, so a bad run never overwrites a good day.
    """
    fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    datasets = fetch_all(per_genre)
    raise_if_failed(validate(datasets, per_genre))
    for dataset, records in datasets.items():
        body = to_jsonl(records, fetched_at, load_date)
        key = put_dataset(s3, bucket, "deezer", dataset, load_date, body)
        print(f"Uploaded s3://{bucket}/{key}")
    return {dataset: len(records) for dataset, records in datasets.items()}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--profile", default=None)
    parser.add_argument("--region", default="us-east-2")
    parser.add_argument("--per-genre", type=int, default=25, help="tracks per genre chart")
    parser.add_argument("--date", default=date.today().isoformat(), help="load date, default today")
    parser.add_argument("--dry-run", action="store_true", help="fetch and print counts, do not upload")
    args = parser.parse_args()

    try:
        if args.dry_run:
            datasets = fetch_all(args.per_genre)
            raise_if_failed(validate(datasets, args.per_genre))
            counts = {name: len(rows) for name, rows in datasets.items()}
        else:
            s3 = boto3.Session(profile_name=args.profile, region_name=args.region).client("s3")
            counts = pull_and_upload(s3, args.bucket, args.date, args.per_genre)
    except (requests.RequestException, DeezerError) as e:
        print(f"Error fetching from Deezer: {e}", file=sys.stderr)
        return 1
    except DataQualityError as e:
        print(f"Data quality check failed, nothing uploaded: {e}", file=sys.stderr)
        return 1
    except ClientError as e:
        print(f"Error uploading to S3: {e}", file=sys.stderr)
        return 1

    print(" ".join(f"{name}={count}" for name, count in counts.items()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
