from google.cloud import bigquery
import logging
from datetime import datetime

def ingest_schedules():
    client = bigquery.Client()

    query = """
    SELECT *
    FROM `bigquery-public-data.baseball.schedules`
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

    df = results.to_dataframe()  
    df["ingestion_time"] = datetime.utcnow()  
    load_job.result() 
    logging.info(f"Data loaded successfully into {table_id}.")
    print(f"✅ Batch ingestion completed at {datetime.now()}")