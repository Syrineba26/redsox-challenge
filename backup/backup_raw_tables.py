from google.cloud import bigquery
import logging
from datetime import datetime


def backup_all_raw_tables():
    client = bigquery.Client()

    # Create timestamp for snapshot table names
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Raw tables to back up
    tables_to_backup = [
        "games_wide",
        "games_post_wide",
        "schedules"
    ]
    try:
        for table in tables_to_backup:
            source_table = f"redsox-bq-project.raw_data.{table}"
            backup_table = f"redsox-bq-project.raw_data_backup.{table}_backup_{timestamp}"

            query = f"""
            CREATE TABLE `{backup_table}` AS
            SELECT *
            FROM `{source_table}`;
            """
            logging.info(f"Backing up: {source_table} → {backup_table}")
            query_job = client.query(query)
            query_job.result()

            print(f"✅ Backup created for {table}: {backup_table}")
    except Exception as e:
        # Log error
        logging.error(f"Backup FAILED: {str(e)}")
        print(f"❌ Backup failed: {str(e)}")
    print("All raw tables have been backed up successfully!")