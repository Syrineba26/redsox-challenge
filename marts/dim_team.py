from google.cloud import bigquery
import logging

def ingest_dim_team():
    client = bigquery.Client()

    query = """
    CREATE OR REPLACE TABLE marts.dim_team AS
    SELECT DISTINCT
    team_id,
    team_name
    FROM (
    SELECT
        home_team_id AS team_id,
        home_team_name AS team_name
    FROM staging.cleaned_all_games_events

    UNION DISTINCT

    SELECT
        away_team_id AS team_id,
        away_team_name AS team_name
        FROM staging.cleaned_all_games_events
    )
    """
    logging.info("Running CREATE OR REPLACE TABLE ...")
    query_job = client.query(query)
    query_job.result()

    logging.info("Table refreshed successfully.")
    print("✅ marts.dim_team created.")