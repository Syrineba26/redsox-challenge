import logging

# Setup logging
def setup_logging():
    logging.basicConfig(
        filename="batch_ingestion.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )