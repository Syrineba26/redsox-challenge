from google.cloud import bigquery
import logging

def ingest_dim_team():
    client = bigquery.Client()

    query = """
    CREATE OR REPLACE TABLE marts.dim_venue AS
    SELECT DISTINCT
    venue_id,
    venue_name,
    venue_city,
    venue_state,
    venue_capacity
    FROM staging.cleaned_all_tables
    """
    logging.info("Running CREATE OR REPLACE TABLE ...")
    query_job = client.query(query)
    query_job.result()

    logging.info("Table refreshed successfully.")
    print("✅ marts.dim_venue created.")
