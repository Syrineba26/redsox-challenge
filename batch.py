from google.cloud import bigquery
import logging
from datetime import datetime
import schedule

from config.logging_config import setup_logging

from raw_ingestion.ingest_games_wide import ingest_games_wide
from raw_ingestion.ingest_games_post_wide import ingest_games_post_wide
from raw_ingestion.ingest_schedules import ingest_schedules

from staging.cleaned_games_wide import ingest_cleaned_games_wide
from staging.cleaned_games_post_wide import ingest_cleaned_games_post_wide
from staging.cleaned_all_games_events import ingest_cleaned_all_games_events
from staging.cleaned_schedules import ingest_cleaned_schedules

from marts.dim_team import ingest_dim_team
from marts.fact_game_summary import ingest_fact_game_summary
from marts.fact_team_performance import ingest_fact_team_performance

from backup.backup_raw_tables import backup_all_raw_tables


if __name__ == "__main__":
    setup_logging()
    ingest_games_wide()
    ingest_games_post_wide()
    ingest_schedules()
    ingest_cleaned_games_post_wide()
    ingest_cleaned_games_wide()
    ingest_cleaned_all_games_events()
    ingest_cleaned_schedules()
    ingest_dim_team()
    ingest_dim_season()
    ingest_dim_venue()
    ingest_fact_game_summary()
    ingest_fact_team_performance()
    backup_all_raw_tables()
