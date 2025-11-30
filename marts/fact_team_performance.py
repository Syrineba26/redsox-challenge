from google.cloud import bigquery
import logging

def ingest_fact_team_performance():
    client = bigquery.Client()

    query = """
    CREATE OR REPLACE TABLE marts.fact_team_performance AS
    WITH all_games AS (
    SELECT season_id, home_team_id AS team_id, home_team_name AS team_name,
           home_final_runs AS runs_scored, 
           away_final_runs AS runs_allowed,
           home_final_hits AS hits, 
           home_final_errors AS errors,
           CASE WHEN home_final_runs > away_final_runs THEN 1 ELSE 0 END AS win,
           CASE WHEN home_final_runs < away_final_runs THEN 1 ELSE 0 END AS loss
    FROM staging.cleaned_all_games_events

    UNION ALL

    SELECT season_id, away_team_id AS team_id, away_team_name AS team_name,
           away_final_runs AS runs_scored, 
           home_final_runs AS runs_allowed,
           away_final_hits AS hits, 
           away_final_errors AS errors,
           CASE WHEN away_final_runs > home_final_runs THEN 1 ELSE 0 END AS win,
           CASE WHEN away_final_runs < home_final_runs THEN 1 ELSE 0 END AS loss
    FROM staging.cleaned_all_games_events
)
SELECT
    season_id,
    team_id,
    team_name,
    COUNT(*) AS games_played,
    SUM(win) AS total_wins,
    SUM(loss) AS total_losses,
    SUM(runs_scored) AS total_runs_scored,
    SUM(runs_allowed) AS total_runs_allowed,
    SUM(hits) AS total_hits,
    SUM(errors) AS total_errors
    FROM cleaned_all_games_events
    GROUP BY season_id, team_id, team_name
    """
    logging.info("Running CREATE OR REPLACE TABLE ...")
    query_job = client.query(query)
    query_job.result()

    logging.info("Table refreshed successfully.")
    print("✅ marts.fact_team_performance created.")