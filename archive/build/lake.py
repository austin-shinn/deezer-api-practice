"""Shared helpers for writing dated JSON Lines snapshots to the S3 raw layer."""
import json


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
