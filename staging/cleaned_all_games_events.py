from google.cloud import bigquery
import logging

def ingest_cleaned_all_games_events():
    client = bigquery.Client()

    query = """
    CREATE OR REPLACE TABLE staging.cleaned_all_games_events AS
    WITH unified AS (
        SELECT * FROM `staging.cleaned_games_wide`
        UNION ALL
        SELECT * FROM `staging.cleaned_games_post_wide`
    ),
    deduped AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY game_id, home_team_id, away_team_id, start_time
                ORDER BY start_time DESC
            ) AS rn
        FROM unified
    )
    SELECT
        game_id,
        season_id,
        season_type,
        year,
        start_time,
        game_status,
        attendance,
        day_night,
        duration_minutes,
        away_team_id,
        away_team_name,
        home_team_id,
        home_team_name,
        venue_id,
        venue_name,
        venue_capacity,
        venue_city,
        venue_state,
        home_final_runs,
        home_final_hits,
        home_final_errors,
        away_final_runs,
        away_final_hits,
        away_final_errors
    FROM deduped
    WHERE rn = 1
    """
    logging.info("Running CREATE OR REPLACE TABLE ...")
    query_job = client.query(query)
    query_job.result()

    logging.info("Table refreshed successfully.")
    print("✅ staging.cleaned_all_games_events updated.")