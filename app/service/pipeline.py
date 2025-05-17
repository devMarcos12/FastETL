import logging
from datetime import datetime
from typing import Dict, Any
import traceback

from .extract import fetch_transaction_products, get_frequency_itemsets
from .transform import product_predominant_profile, most_common_products
from .load import load_product_predominant_profile, load_most_common_products

logger = logging.getLogger(__name__)

class ETLPipeline:
    """ Orquestrates the ETL process for the application. """

    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.metrics = {
            "records_processed": {},
            "duration": {}
        }

    def run(self) -> Dict[str, Any]:
        """
        Execute the ETL pipeline.

        Returns:
            Dict: A dictionary containing the status of the ETL process,
        """
        self.start_time = datetime.now()
        logger.info(f"Starting ETL pipeline at {self.start_time}")

        try:
            # Extract
            extract_start = datetime.now()
            transaction_data = fetch_transaction_products()
            frequency_data = get_frequency_itemsets()
            extract_end = datetime.now()
            self.metrics["duration"]["extract"] = (extract_end - extract_start).total_seconds()
            self.metrics["records_processed"]["transactions"] = len(transaction_data)
            self.metrics["records_processed"]["frequency_itemsets"] = len(frequency_data)

            # Transform
            transform_start = datetime.now()
            predominant_profile = product_predominant_profile(transaction_data)
            most_common = most_common_products(frequency_data)
            transform_end = datetime.now()
            self.metrics["duration"]["transform"] = (transform_end - transform_start).total_seconds()

            # Load
            load_start = datetime.now()
            profile_result = load_product_predominant_profile(predominant_profile)
            common_result = load_most_common_products(most_common)
            load_end = datetime.now()
            self.metrics["duration"]["load"] = (load_end - load_start).total_seconds()

            self.end_time = datetime.now()
            total_duration = (self.end_time - self.start_time).total_seconds()

            result = {
                "status": "success" if profile_result and common_result else "partial_success",
                "start_time": self.start_time.isoformat(),
                "end_time": self.end_time.isoformat(),
                "total_duration_seconds": total_duration,
                "metrics": self.metrics
            }

            logger.info(f"ETL pipeline completed in {total_duration:.2f} seconds")
            return result

        except Exception as e:
            self.end_time = datetime.now()
            error_details = traceback.format_exc()
            logger.error(f"ETL pipeline failed: {str(e)}\n{error_details}")

            return {
                "status": "failed",
                "error": str(e),
                "error_details": error_details,
                "start_time": self.start_time.isoformat() if self.start_time else None,
                "end_time": self.end_time.isoformat(),
                "metrics": self.metrics
            }