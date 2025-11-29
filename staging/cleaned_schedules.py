from google.cloud import bigquery
import logging

def ingest_cleaned_schedules():
    client = bigquery.Client()

    query = """
    CREATE OR REPLACE TABLE `redsox-bq-project.staging.cleaned_schedules` AS
    SELECT
    gameId AS game_id,
    gameNumber AS game_number,
    seasonId AS season_id,
    year,
    type AS game_type,
    dayNight AS day_night,
    duration_minutes,
    homeTeamId AS home_team_id,
    homeTeamName AS home_team_name,
    awayTeamId AS away_team_id,
    awayTeamName AS away_team_name,
    CAST(startTime AS TIMESTAMP) AS start_time,
    attendance,
    status
    FROM `redsox-bq-project.raw_data.schedules`
    WHERE gameId IS NOT NULL
    """
    logging.info("Running CREATE OR REPLACE TABLE ...")
    query_job = client.query(query)
    query_job.result()

    logging.info("Table refreshed successfully.")
    print("✅ staging.cleaned_schedules updated.")
