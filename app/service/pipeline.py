import logging
from datetime import datetime
from typing import Dict, Any
import traceback

from .extract import fetch_transaction_products, get_frequency_itemsets, extract_orders_with_customers_and_items
from .transform import product_predominant_profile, most_common_products, transform_complete_orders_to_dw_format
from .load import load_product_predominant_profile, load_most_common_products, load_complete_orders_to_dw

logger = logging.getLogger(__name__)

class ETLPipeline:
    """ Orquestrates the ETL process for the application. """

    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.metrics = {
            "records_processed": {},
            "duration": {},
            "success": {}
        }

    def _run_etl_step(self, name, extract_fn, transform_fn, load_fn):
        """Helper method to run a single ETL flow"""
        step_metrics = {}

        # Extract
        extract_start = datetime.now()
        raw_data = extract_fn()
        extract_end = datetime.now()
        step_metrics["extract_duration"] = (extract_end - extract_start).total_seconds()
        step_metrics["records_extracted"] = len(raw_data) if hasattr(raw_data, '__len__') else 0

        # Transform
        transform_start = datetime.now()
        transformed_data = transform_fn(raw_data)
        transform_end = datetime.now()
        step_metrics["transform_duration"] = (transform_end - transform_start).total_seconds()

        # Load
        load_start = datetime.now()
        load_success = load_fn(transformed_data)
        load_end = datetime.now()
        step_metrics["load_duration"] = (load_end - load_start).total_seconds()
        step_metrics["success"] = load_success

        return step_metrics, load_success

    def run(self) -> Dict[str, Any]:
        """Execute the ETL pipeline."""
        self.start_time = datetime.now()
        logger.info(f"Starting ETL pipeline at {self.start_time}")

        try:
            # Define ETL flows
            etl_flows = [
                {
                    "name": "product_profile",
                    "extract": fetch_transaction_products,
                    "transform": product_predominant_profile,
                    "load": load_product_predominant_profile
                },
                {
                    "name": "common_products",
                    "extract": get_frequency_itemsets,
                    "transform": most_common_products,
                    "load": load_most_common_products
                },
                {
                    "name": "orders",
                    "extract": extract_orders_with_customers_and_items,
                    "transform": transform_complete_orders_to_dw_format,
                    "load": load_complete_orders_to_dw
                }
            ]

            # Run each ETL flow
            all_success = True
            for flow in etl_flows:
                logger.info(f"Running ETL flow: {flow['name']}")
                metrics, success = self._run_etl_step(
                    flow["name"],
                    flow["extract"],
                    flow["transform"],
                    flow["load"]
                )
                self.metrics[flow["name"]] = metrics
                all_success = all_success and success

            self.end_time = datetime.now()
            total_duration = (self.end_time - self.start_time).total_seconds()

            result = {
                "status": "success" if all_success else "partial_success",
                "start_time": self.start_time.isoformat(),
                "end_time": self.end_time.isoformat(),
                "total_duration_seconds": total_duration,
                "metrics": self.metrics
            }

            logger.info(f"ETL pipeline completed in {total_duration:.2f} seconds. Status: {result['status']}")
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