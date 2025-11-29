from google.cloud import bigquery
import logging

def ingest_fact_game_summary():
    client = bigquery.Client()

    query = """
    CREATE OR REPLACE TABLE marts.fact_game_summary AS
    SELECT
    game_id,
    season_id,
    year,
    start_time,
    game_status,
    attendance,
    day_night,
    duration_minutes,
    home_team_id,
    home_team_name,
    away_team_id,
    away_team_name,
    venue_id,
    venue_name,
    venue_capacity,
    venue_city,
    venue_state,
    home_final_runs,
    away_final_runs,
    home_final_hits,
    away_final_hits,
    home_final_errors,
    away_final_errors
    FROM staging.cleaned_all_games_events
    """
    logging.info("Running CREATE OR REPLACE TABLE ...")
    query_job = client.query(query)
    query_job.result()

    logging.info("Table refreshed successfully.")
    print("✅ marts.fact_game_summary created.")