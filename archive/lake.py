"""Shared helpers for writing dated JSON Lines snapshots to the S3 raw layer."""
import json

XREF_KEY = "reference/artist_xref/artist_xref.jsonl"

# A source may return slightly fewer rows than requested on a given day; below this share of the
# expected count, the pull is treated as broken and nothing is uploaded.
MIN_ROW_FRACTION = 0.9
MAX_NULL_FRACTION = 0.05


class DataQualityError(Exception):
    """Raised when fetched data fails a sanity check, so a bad day is never written to S3."""


def raise_if_failed(problems: list[str]) -> None:
    if problems:
        raise DataQualityError("; ".join(problems))


def check_row_count(name: str, actual: int, expected: int) -> list[str]:
    if actual < expected * MIN_ROW_FRACTION:
        return [f"{name}: {actual} rows, expected about {expected}"]
    return []


def check_null_share(name: str, nulls: int, total: int) -> list[str]:
    if total and nulls / total > MAX_NULL_FRACTION:
        return [f"{name}: {nulls} of {total} rows are null"]
    return []
TRUSTED_XREF_METHODS = ("name_normalized", "lastfm_lookup")


def load_xref_artists(s3, bucket: str) -> list[tuple[str, str | None]]:
    """Return (lastfm_name, mbid) for each trusted Deezer-to-Last.fm match; empty if no file yet."""
    try:
        body = s3.get_object(Bucket=bucket, Key=XREF_KEY)["Body"].read().decode("utf-8")
    except s3.exceptions.NoSuchKey:
        return []
    records = [json.loads(line) for line in body.splitlines() if line.strip()]
    return [
        (r["lastfm_name"], r.get("mbid"))
        for r in records
        if r["match_method"] in TRUSTED_XREF_METHODS and r.get("lastfm_name")
    ]


def build_key(source: str, dataset: str, load_date: str, filename: str) -> str:
    return f"raw/{source}/{dataset}/dt={load_date}/{filename}"


def to_jsonl(records: list[dict], fetched_at: str, load_date: str) -> bytes:
    lines = [
        json.dumps({**r, "load_date": load_date, "fetched_at": fetched_at}, ensure_ascii=False)
        for r in records
    ]
    return ("\n".join(lines) + "\n").encode("utf-8")


def put_dataset(s3, bucket: str, source: str, dataset: str, load_date: str, body: bytes) -> str:
    key = build_key(source, dataset, load_date, f"{dataset}.jsonl")
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/x-ndjson")
    return key
