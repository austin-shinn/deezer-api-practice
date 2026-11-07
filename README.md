# api-practice

A practice notebook for pulling data from the public [Deezer API](https://developers.deezer.com/api)

The notebook (`deezer_api_practice.ipynb`) covers:

- fetching Deezer genres and per-genre chart tracks
- handling errors
- retrying rate-limited calls
- turning API responses into DataFrames
- uploading to S3 bucket in AWS

Goals are to 1. schedule daily runs in Lambda then 2. pull and transform in Athena, 3. creating some visualizations with the data.
