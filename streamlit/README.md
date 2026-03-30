# Streamlit Frontend

Simple read-only frontend for the LATAM roles pipeline.

## What it shows

- role families from `positions.json`
- latest month total positions
- top technologies for the latest month
- monthly positions trend
- monthly technology coverage trend

## Run locally

```bash
make streamlit-install
make streamlit-run
```

The app expects AWS credentials and `OUTPUT_BUCKET` in `.env.dev`.

## Notes

- Data is loaded directly from S3.
- The UI does not require Airflow to be running.

## Streamlit Community Cloud

The app can run on Streamlit Community Cloud using read-only AWS credentials.

Add these secrets in the Streamlit app settings:

```toml
AWS_ACCESS_KEY_ID="..."
AWS_SECRET_ACCESS_KEY="..."
AWS_REGION="us-east-1"
OUTPUT_BUCKET="latam-roles-results-dev"
```

Optional:

```toml
AWS_SESSION_TOKEN="..."
```

The bucket should stay private. The Streamlit app reads it server-side using
the read-only IAM user credentials above.
