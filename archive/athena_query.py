"""Run an Athena query in the music-trends workgroup and print the rows as a table."""
import argparse
import sys
import time

import boto3
from botocore.exceptions import ClientError

POLL_SECONDS = 1.0
TERMINAL_STATES = {"SUCCEEDED", "FAILED", "CANCELLED"}


def run_query(athena, sql: str, database: str, workgroup: str) -> str:
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        WorkGroup=workgroup,
    )
    query_id = response["QueryExecutionId"]
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]
        if status["State"] in TERMINAL_STATES:
            break
        time.sleep(POLL_SECONDS)
    if status["State"] != "SUCCEEDED":
        raise RuntimeError(f"Query {status['State']}: {status.get('StateChangeReason', '')}")
    return query_id


def fetch_rows(athena, query_id: str) -> list[list[str]]:
    rows: list[list[str]] = []
    paginator = athena.get_paginator("get_query_results")
    for page in paginator.paginate(QueryExecutionId=query_id):
        for row in page["ResultSet"]["Rows"]:
            rows.append([col.get("VarCharValue", "") for col in row["Data"]])
    return rows


def print_table(rows: list[list[str]]) -> None:
    if not rows:
        return
    widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]))]
    for index, row in enumerate(rows):
        print("  ".join(cell.ljust(width) for cell, width in zip(row, widths)))
        if index == 0:
            print("  ".join("-" * width for width in widths))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sql")
    parser.add_argument("--database", default="music_trends")
    parser.add_argument("--workgroup", default="music-trends")
    parser.add_argument("--profile", default=None)
    parser.add_argument("--region", default="us-east-2")
    args = parser.parse_args()

    athena = boto3.Session(profile_name=args.profile, region_name=args.region).client("athena")
    try:
        query_id = run_query(athena, args.sql, args.database, args.workgroup)
        print_table(fetch_rows(athena, query_id))
    except (ClientError, RuntimeError) as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
