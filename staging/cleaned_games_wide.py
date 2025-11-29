from google.cloud import bigquery
import logging

def ingest_cleaned_games_wide():
    client = bigquery.Client()

    query = """
    CREATE OR REPLACE TABLE `redsox-bq-project.staging.cleaned_games_wide` AS
    SELECT
        gameId AS game_id,
        seasonId AS season_id,
        seasonType AS season_type,
        year,
        CAST(startTime AS TIMESTAMP) AS start_time,
        gameStatus AS game_status,
        attendance,
        dayNight AS day_night,
        durationMinutes AS duration_minutes,
        homeTeamId AS home_team_id,
        homeTeamName AS home_team_name,
        awayTeamId AS away_team_id,
        awayTeamName AS away_team_name,
        venueId AS venue_id,
        venueName AS venue_name,
        venueCity AS venue_city,
        venueState AS venue_state,
        venueCapacity AS venue_capacity,
        homeFinalRuns AS home_final_runs,
        homeFinalHits AS home_final_hits,
        homeFinalErrors AS home_final_errors,
        awayFinalRuns AS away_final_runs,
        awayFinalHits AS away_final_hits,
        awayFinalErrors AS away_final_errors
        FROM `redsox-bq-project.raw_data.games_wide`
        WHERE gameId IS NOT NULL

        """

    logging.info("Running CREATE OR REPLACE TABLE ...")
    query_job = client.query(query)
    query_job.result()

    logging.info("Table refreshed successfully.")
    print("✅ staging.cleaned_games_wide updated.")