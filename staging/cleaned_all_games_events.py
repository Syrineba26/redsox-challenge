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
            ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY start_time DESC) AS rn
        FROM unified
    )
    SELECT
    d.game_id,
    d.season_id,
    d.season_type,
    d.year,
    d.start_time AS game_start_time,
    d.game_status,
    d.attendance,
    d.day_night,
    d.duration_minutes,
    d.away_team_id,
    d.away_team_name,
    d.home_team_id,
    d.home_team_name,
    d.home_final_runs,
    d.home_final_hits,
    d.home_final_errors,
    d.away_final_runs,
    d.away_final_hits,
    d.away_final_errors,
    d.venue_id,
    d.venue_name,
    d.venue_city,
    d.venue_state,
    d.venue_capacity,
    
    FROM deduped d
    LEFT JOIN `staging.cleaned_schedules` s
        ON d.game_id = s.game_id
    WHERE rn = 1
    """
    logging.info("Running CREATE OR REPLACE TABLE ...")
    query_job = client.query(query)
    query_job.result()

    logging.info("Table refreshed successfully.")
    print("✅ staging.cleaned_all_games_events updated.")