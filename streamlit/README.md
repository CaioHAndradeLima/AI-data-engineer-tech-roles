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
