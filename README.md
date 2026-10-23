# cricket-data-analytics

Finished Cricbuzz matches are stored in the Databricks catalog `cricsocc`, schema `bronze`.

GitHub Actions runs `.github/workflows/databricks-daily.yml` every day at 00:00 Asia/Dhaka (18:00 UTC). The workflow reads the `DATABRICKS_TOKEN` secret and only inserts matches that are not already in `cricsocc.bronze.matches`.
