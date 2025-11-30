from google.cloud import bigquery

def has_new_data():
    client = bigquery.Client()

    tables = [
        "raw_data.games_wide",
        "raw_data.games_post_wide",
        "raw_data.schedules"
    ]

    for table in tables:
        query = f"""
            SELECT COUNT(*) AS new_rows
            FROM `{table}`
            WHERE DATE(ingestion_time) = CURRENT_DATE()
        """

        result = client.query(query).result()
        row = list(result)[0]

        if row.new_rows > 0:
            print(f"🔹 New data found in: {table}")
            return True

    print("⚠️ No new data found in any raw table.")
    return False
