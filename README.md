# api-practice

A practice notebook for pulling data from the public [Deezer API](https://developers.deezer.com/api) with `requests` and shaping it with `pandas`.

The notebook (`api_practice.ipynb`) covers:

- fetching Deezer genres and per-genre chart tracks
- handling errors, including Deezer's habit of returning errors inside a 200 response
- retrying rate-limited calls
- turning API responses into DataFrames

Deezer's public endpoints need no API key. The cells that use `boto3` need AWS credentials on your machine.

## Setup

```bash
git clone https://github.com/shinnsplints/api-practice.git
cd api-practice
py -m venv .venv
.venv\Scripts\Activate.ps1
py -m pip install -r requirements.txt
```

Then open `api_practice.ipynb` in VS Code (with the Python and Jupyter extensions) or JupyterLab, and select the `.venv` kernel.

On macOS or Linux, use `python3` and `source .venv/bin/activate` instead of the Windows commands.

## AWS

Only the S3 cells need AWS access. Sign in with the AWS CLI (for example `aws login`) or your usual profile. Credentials stay on your machine and are not stored in this repo.
