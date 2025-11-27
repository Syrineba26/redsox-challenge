from google.cloud import bigquery
import logging
from datetime import datetime
import schedule


# Setup logging
logging.basicConfig(
    filename="batch_ingestion.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def ingest_games_wide():
    client = bigquery.Client()

    # Query public dataset
    query = """
    SELECT *
    FROM `bigquery-public-data.baseball.games_wide`
    LIMIT 20
    """

    logging.info("Running query on public dataset...")
    query_job = client.query(query)
    results = query_job.result()
    logging.info("Query completed successfully.")

    # Configure destination table
    table_id = "redsox-bq-project.raw_data.games_wide"

    # Load results into your table in batch
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Overwrite table each run
    )

    logging.info(f"Loading data into {table_id} ...")
    load_job = client.load_table_from_dataframe(
        results.to_dataframe(),  # Convert to Pandas DataFrame
        table_id,
        job_config=job_config
    )

    load_job.result()  # Wait for the load to finish
    logging.info(f"Data loaded successfully into {table_id}.")
    print(f"✅ Batch ingestion completed at {datetime.now()}")

def ingest_games_post_wide():
    client = bigquery.Client()

    query = """
    SELECT *
    FROM `bigquery-public-data.baseball.games_post_wide`
    LIMIT 20
    """

    logging.info("Running query on public dataset...")
    query_job = client.query(query)
    results = query_job.result()
    logging.info("Query completed successfully.")

    table_id = "redsox-bq-project.raw_data.games_post_wide"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE 
    )

    logging.info(f"Loading data into {table_id} ...")
    load_job = client.load_table_from_dataframe(
        results.to_dataframe(),  
        table_id,
        job_config=job_config
    )

    load_job.result() 
    logging.info(f"Data loaded successfully into {table_id}.")
    print(f"✅ Batch ingestion completed at {datetime.now()}")

def ingest_schedules():
    client = bigquery.Client()

    query = """
    SELECT *
    FROM `bigquery-public-data.baseball.schedules`
    LIMIT 20
    """

    logging.info("Running query on public dataset...")
    query_job = client.query(query)
    results = query_job.result()
    logging.info("Query completed successfully.")

    table_id = "redsox-bq-project.raw_data.schedules"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  
    )

    logging.info(f"Loading data into {table_id} ...")
    load_job = client.load_table_from_dataframe(
        results.to_dataframe(), 
        table_id,
        job_config=job_config
    )

    load_job.result() 
    logging.info(f"Data loaded successfully into {table_id}.")
    print(f"✅ Batch ingestion completed at {datetime.now()}")

def ingest_cleaned_games_post_wide():
    client = bigquery.Client()

    query = """
    CREATE OR REPLACE TABLE `redsox-bq-project.staging.cleaned_games_post_wide` AS
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
        FROM `redsox-bq-project.raw_data.games_post_wide`
        WHERE gameId IS NOT NULL
        LIMIT 20
        """

    logging.info("Running CREATE OR REPLACE TABLE ...")
    query_job = client.query(query)
    query_job.result()

    logging.info("Table refreshed successfully.")
    print("✅ staging.cleaned_games_post_wide updated.")

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
        LIMIT 20
        """

    logging.info("Running CREATE OR REPLACE TABLE ...")
    query_job = client.query(query)
    query_job.result()

    logging.info("Table refreshed successfully.")
    print("✅ staging.cleaned_games_wide updated.")

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
    LIMIT 20
    """

    logging.info("Running CREATE OR REPLACE TABLE ...")
    query_job = client.query(query)
    query_job.result()

    logging.info("Table refreshed successfully.")
    print("✅ staging.cleaned_schedules updated.")

def backup_all_raw_tables():
    client = bigquery.Client()

    # Create timestamp for snapshot table names
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Raw tables to back up → add more if needed
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

schedule.every().day.at("02:00").do(backup_all_raw_tables)
schedule.every().day.at("02:00").do(ingest_games_wide)
schedule.every().day.at("02:00").do(ingest_games_post_wide)
schedule.every().day.at("02:00").do(ingest_schedules)

if __name__ == "__main__":
    ingest_games_wide()
    ingest_games_post_wide()
    ingest_schedules()
    ingest_cleaned_games_post_wide()
    ingest_cleaned_games_wide()
    ingest_cleaned_schedules()
    backup_all_raw_tables()

#while True:
#    schedule.run_pending()
