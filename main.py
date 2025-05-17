import logging
import os
import json
from datetime import datetime
from app.service.pipeline import ETLPipeline

def setup_logging():
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%d%m%Y_%H%M")
    log_file = f"{log_dir}/etl_{timestamp}.log"

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

if __name__ == "__main__":
    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("Starting ETL application")

    pipeline = ETLPipeline()
    result = pipeline.run()

    os.makedirs("reports", exist_ok=True)
    timestamp = datetime.now().strftime("%d%m%Y_%H%M")

    with open(f"reports/etl_result_{timestamp}.json", "w") as f:
        json.dump(result, f, indent=2)

    if result["status"] == "success":
        logger.info(f"ETL completed successfully in {result['total_duration_seconds']:.2f} seconds")
    elif result["status"] == "partial_success":
        logger.warning("ETL completed with partial success")
    else:
        logger.error(f"ETL failed: {result.get('error', 'Unknown error')}")