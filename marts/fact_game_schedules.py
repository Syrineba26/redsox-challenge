from google.cloud import bigquery
import logging

def ingest_fact_game_schedules():
    client = bigquery.Client()

    query = """
    CREATE OR REPLACE TABLE marts.fact_game_schedules AS
    SELECT
        game_id,
        game_number,
        season_id,
        year,
        game_type AS type,
        start_time,
        day_night,
        duration_minutes,
        attendance,
        home_team_id,
        home_team_name,
        away_team_id,
        away_team_name
    FROM staging.cleaned_schedules
    """
    logging.info("Running CREATE OR REPLACE TABLE ...")
    query_job = client.query(query)
    query_job.result()

    logging.info("Table refreshed successfully.")
    print("✅ marts.fact_game_schedules created.")