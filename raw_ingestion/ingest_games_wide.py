from google.cloud import bigquery
import logging
from datetime import datetime

def ingest_games_wide():
    client = bigquery.Client()

    # Query public dataset
    query = """
    SELECT *
    FROM `bigquery-public-data.baseball.games_wide`
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