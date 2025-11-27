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

    # 1️⃣ Query public dataset
    query = """
    SELECT *
    FROM `bigquery-public-data.baseball.games_wide`
    LIMIT 20
    """

    logging.info("Running query on public dataset...")
    query_job = client.query(query)
    results = query_job.result()
    logging.info("Query completed successfully.")

    # 2️⃣ Configure destination table
    table_id = "redsox-bq-project.raw_data.games_wide"

    # 3️⃣ Load results into your table in batch
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


def ingest_schedules():
    client = bigquery.Client()

    # 1️⃣ Query public dataset
    query = """
    SELECT *
    FROM `bigquery-public-data.baseball.schedules`
    LIMIT 10
    """

    logging.info("Running query on public dataset...")
    query_job = client.query(query)
    results = query_job.result()
    logging.info("Query completed successfully.")

    # 2️⃣ Configure destination table
    table_id = "redsox-bq-project.raw_data.schedules"

    # 3️⃣ Load results into your table in batch
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

schedule.every().day.at("19:35").do(ingest_games_wide)
schedule.every().day.at("19:35").do(ingest_schedules)

if __name__ == "__main__":
    ingest_games_wide()

    
#while True:
#    schedule.run_pending()
    
