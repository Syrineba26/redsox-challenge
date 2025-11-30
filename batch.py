from google.cloud import bigquery
import logging
from datetime import datetime
import schedule

from config.logging_config import setup_logging

from utils.check_new_data import has_new_data

from raw_ingestion.ingest_games_wide import ingest_games_wide
from raw_ingestion.ingest_games_post_wide import ingest_games_post_wide
from raw_ingestion.ingest_schedules import ingest_schedules

from staging.cleaned_games_wide import ingest_cleaned_games_wide
from staging.cleaned_games_post_wide import ingest_cleaned_games_post_wide
from staging.cleaned_all_games_events import ingest_cleaned_all_games_events
from staging.cleaned_schedules import ingest_cleaned_schedules

from marts.dim_season import ingest_dim_season
from marts.dim_venue import ingest_dim_venue
from marts.fact_game_summary import ingest_fact_game_summary
from marts.fact_team_performance import ingest_fact_team_performance

from backup.backup_raw_tables import backup_all_raw_tables


if __name__ == "__main__":
    setup_logging()
    # Check new rows before ingestion
    if not has_new_data():
        logging.info("No new data → stopping pipeline.")
        exit()

    # RAW ingestion
    ingest_games_wide()
    ingest_games_post_wide()
    ingest_schedules()
    # Staging
    ingest_cleaned_games_post_wide()
    ingest_cleaned_games_wide()
    ingest_cleaned_all_games_events()
    ingest_cleaned_schedules()
    # Marts
    ingest_dim_season()
    ingest_dim_venue()
    ingest_fact_game_summary()
    ingest_fact_team_performance()
    # Backup
    backup_all_raw_tables()
