from google.cloud import bigquery
import logging

def ingest_dim_season():
    client = bigquery.Client()

    query = """
    CREATE OR REPLACE TABLE marts.dim_season AS
    SELECT DISTINCT
    season_id,
    season_type,
    year
    FROM staging.cleaned_all_games_events

    """
    logging.info("Running CREATE OR REPLACE TABLE ...")
    query_job = client.query(query)
    query_job.result()

    logging.info("Table refreshed successfully.")
    print("✅ marts.dim_season created.")