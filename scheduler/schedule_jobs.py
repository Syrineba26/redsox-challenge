import schedule
import time

from raw_ingestion.ingest_games_wide import ingest_games_wide
from raw_ingestion.ingest_games_post_wide import ingest_games_post_wide
from raw_ingestion.ingest_schedules import ingest_schedules

from staging.cleaned_games_wide import ingest_cleaned_games_wide
from staging.cleaned_games_post_wide import ingest_cleaned_games_post_wide
from staging.ingest_cleaned_all_games_events import ingest_cleaned_all_games_events
from staging.cleaned_schedules import ingest_cleaned_schedules

from marts.dim_season import ingest_dim_season
from marts.dim_venue import ingest_dim_venue
from marts.ingest_fact_game_summary import ingest_fact_game_summary
from marts.ingest_fact_team_performance import ingest_fact_team_performance

def schedule_all_jobs():
    schedule.every().day.at("02:00").do(backup_all_raw_tables)
    schedule.every().day.at("02:00").do(ingest_games_wide)
    schedule.every().day.at("02:00").do(ingest_games_post_wide)
    schedule.every().day.at("02:00").do(ingest_schedules)
    schedule.every().day.at("02:00").do(ingest_cleaned_games_post_wide)
    schedule.every().day.at("02:00").do(ingest_cleaned_games_wide)
    schedule.every().day.at("02:00").do(ingest_cleaned_schedules)
    schedule.every().day.at("02:00").do(ingest_cleaned_all_games_events)
    schedule.every().day.at("02:00").do(ingest_dim_season)
    schedule.every().day.at("02:00").do(ingest_dim_venue)
    schedule.every().day.at("02:00").do(ingest_fact_game_summary)
    schedule.every().day.at("02:00").do(ingest_fact_team_performance)
    
    # while True:
    #     schedule.run_pending()