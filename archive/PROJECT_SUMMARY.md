# Music trends pipeline: project summary

Written 2026-11-03 so the work can be resumed on another PC. Contains no secrets. The Last.fm API key is stored in AWS SSM Parameter Store, not in any file.

## Goal
A data engineering portfolio project that collects artist popularity and genre data daily, keeps it as dated snapshots in S3, makes it queryable with SQL, and will feed a dashboard. It is meant to exercise the job-search skills: AWS (S3, Athena, Glue, Lambda/EventBridge, later Redshift and MWAA), SQL, Python, dbt, Snowflake, and optionally Looker.

Study order decided earlier: SQL (with daily Python practice), then AWS (S3, Athena, Glue, Redshift, MWAA), then dbt and Snowflake together, then Looker.

## Current state (built and deployed)
Everything runs in AWS region **us-east-2**, AWS account **383344224200**, using the CLI profile **`spotify-pipeline`** (created with `aws login --profile spotify-pipeline`).

Data sources in use:
- **Deezer API** (no key): genres, per-genre top-25 track charts, artist fan counts.
- **Last.fm API** (free key in SSM): tag top artists, global chart, listeners and play counts per artist.
- **Not used yet:** Spotify (endpoints not yet verified), MusicBrainz, Wikipedia pageviews, Ticketmaster, Kaggle backfill files.

Findings worth remembering:
- Deezer's `/chart/<genre>/artists` and `/genre/<genre>/artists` ignore the genre and return the overall chart. Only `/chart/<genre>/tracks` varies, so genre membership is derived from track charts.
- Deezer genre charts are quirky (the Rock chart includes Cab Calloway). Last.fm tags are noisy and user-generated.
- None of the APIs give history, so the timeline starts 2026-11-02 and grows one snapshot a day.

## AWS resources (all CloudFormation stacks, us-east-2)
| Stack | What it creates |
|---|---|
| `music-trends-deezer` | Lambda `music-trends-deezer-deezer-pull`, EventBridge schedule daily 10:00 UTC |
| `music-trends-lastfm` | Lambda `music-trends-lastfm-lastfm-pull`, schedule daily 10:30 UTC |
| `music-trends-xref` | Lambda `music-trends-xref-xref-refresh`, schedule Sundays 12:00 UTC |
| `music-trends-athena` | Glue database `music_trends` with 7 tables, Athena workgroup `music-trends` |
| `music-trends-alerts` | SNS email topic, error alarms for all three Lambdas, no-run alarms for the two daily ones |

Other resources:
- **S3 bucket `lastfm-pipeline-data`** (us-east-2, public access blocked, SSE-S3, 30-day expiry on `athena-results/`).
  - `raw/deezer/<dataset>/dt=YYYY-MM-DD/<dataset>.jsonl` (genres, genre_chart_tracks, artists)
  - `raw/lastfm/<dataset>/dt=YYYY-MM-DD/<dataset>.jsonl` (tag_top_artists, chart_top_artists, artists)
  - `reference/artist_xref/artist_xref.jsonl` (Deezer to Last.fm artist matches)
  - `deploy/*.zip` (Lambda code packages)
  - `athena-results/` (query output)
- **SSM SecureString `/music-pipeline/lastfm-api-key`** holds the Last.fm key.
- **Athena tables:** `deezer_genres`, `deezer_genre_chart_tracks`, `deezer_artists`, `lastfm_tag_top_artists`, `lastfm_chart_top_artists`, `lastfm_artists`, `artist_xref`. Daily tables use partition projection on `dt`, so new days appear automatically.
- A second, unexplained bucket `neon-snake-383344224200` exists (one 4 KB object). Not created by this project; check before deleting.

## Code files (in the accompanying zip)
| File | Purpose |
|---|---|
| `lake.py` | Shared helpers: S3 key layout, JSONL writer, data-quality checks, xref loader |
| `deezer_pull.py`, `deezer_handler.py` | Deezer fetch + validate + upload; Lambda entry point |
| `lastfm_pull.py`, `lastfm_handler.py` | Last.fm fetch + validate + upload; Lambda entry point |
| `build_artist_xref.py`, `xref_handler.py` | Builds and refreshes the Deezer-to-Last.fm artist cross-reference |
| `build_lambda.py` | Builds the Lambda zips (`deezer`, `lastfm`, `xref`) for Linux x86_64, Python 3.13 |
| `athena_query.py` | Runs an Athena query from the command line and prints a table |
| `upload_snapshot.py` | Early helper that uploads a single local file to a dated S3 key |
| `test_validation.py` | 15 unit tests for the row-count checks |
| `deezer-schedule.yaml`, `lastfm-schedule.yaml`, `xref-schedule.yaml` | CloudFormation for each Lambda, role, log group and schedule |
| `athena.yaml` | CloudFormation for the workgroup, database and 7 tables |
| `alerts.yaml` | CloudFormation for the SNS topic and alarms |

## Setting up on another PC
1. Install Python 3.13 or newer and AWS CLI 2.32 or newer.
2. Install packages: `pip install boto3 "botocore[crt]" requests` (`botocore[crt]` is required for `aws login` credentials).
3. Sign in and set the Region (the profile must use us-east-2):
```bash
aws login --profile spotify-pipeline
aws configure set region us-east-2 --profile spotify-pipeline
```
4. Unzip the code into one folder and test:
```bash
python -m unittest -v test_validation
python athena_query.py "SELECT dt, count(*) FROM deezer_artists GROUP BY dt ORDER BY dt" --profile spotify-pipeline
```
5. The Last.fm key already lives in SSM in the account, so local runs of `lastfm_pull.py` find it through your AWS login.

## Useful commands
Run a pull locally (add `--dry-run` to fetch and check without uploading):
```bash
python deezer_pull.py --bucket lastfm-pipeline-data --profile spotify-pipeline
python lastfm_pull.py --bucket lastfm-pipeline-data --profile spotify-pipeline
python build_artist_xref.py --bucket lastfm-pipeline-data --profile spotify-pipeline
```
Deploy a code change (example for Deezer; use `lastfm` or `xref` for the others):
```bash
python build_lambda.py deezer
aws s3 cp dist/deezer_pull.zip s3://lastfm-pipeline-data/deploy/deezer_pull.zip --profile spotify-pipeline --region us-east-2
aws lambda update-function-code --function-name music-trends-deezer-deezer-pull --s3-bucket lastfm-pipeline-data --s3-key deploy/deezer_pull.zip --profile spotify-pipeline --region us-east-2
```
Template changes: `aws cloudformation deploy --stack-name <stack> --template-file <file>.yaml --capabilities CAPABILITY_IAM ...` (see each file's parameters; `athena.yaml` needs `DataBucket=lastfm-pipeline-data`, `alerts.yaml` needs `AlertEmail=<your address>`).

Lesson learned: do not wait on long Lambda runs with a synchronous `aws lambda invoke`. The connection can drop and the CLI retries, re-running the function. Use `--invocation-type Event` and read the CloudWatch logs.

Where to look at the data: Athena console in us-east-2, workgroup `music-trends`, database `music_trends`. Set the workgroup first or Athena asks for a query result location.

## Open items
1. **Confirm the scheduled runs fire on their own** (10:00 and 10:30 UTC). All data so far came from manual test runs, so the scheduler has not been proven yet.
2. **Last.fm 100 MB storage cap.** Current rate is about 380 KB a day, so roughly 9 months to reach it. Plan: convert to Parquet and drop unused fields.
3. **Delete the stray test file** `raw/deezer/genres/dt=2026-11-02/genres.json` (it adds one junk row to `deezer_genres`).
4. **Four artist matches held for review** (Roedelius, The Citizens of Halloween, Chrystal, HUGEL) are marked `candidate_review` and excluded from joins.
5. **Last.fm run time is about 7 minutes** against the 15-minute Lambda limit and grows with the number of matched artists.
6. **Lambda retry behavior:** a failed run can retry several times (scheduler plus Lambda async retries). Same-day reruns are safe but cost extra Last.fm calls. Consider capping.
7. **Housekeeping:** the old `default` AWS profile has dead access keys (deactivate if they belong to an IAM user); the Last.fm key was typed on a command line and pasted into chat, so consider clearing shell history and regenerating it.
8. **Code is not version controlled.** Put it in a git repository, with a README and the architecture diagram.

## Possible next steps
- Dashboard (timeline, current chart, Deezer versus Last.fm comparison). Last.fm's terms require attribution and a link wherever its data is shown, and the free license is non-commercial only.
- Wait a week or two so the timeline has data.
- dbt models on Athena (cleaned artist and genre tables, snapshots for slowly changing fields).
- Verify which Spotify endpoints a new developer app can use, then add Spotify as a third source.
- Add MusicBrainz IDs for stronger cross-source matching.
- Later skill coverage: Glue jobs, Redshift, MWAA/Airflow, Snowflake, Looker.
