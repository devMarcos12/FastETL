import logging
from datetime import datetime
from typing import Dict, Any
from ..db.mongo_connection import mongo_context

logger = logging.getLogger(__name__)

def load_product_predominant_profile(
        data: Dict[str, Any],
        db_name: str = 'DW-MarcosJunior',
        collection_name: str = 'ETL-predominant_profile'
    ) -> bool:
    """
    Load the product predominant profile data into MongoDB.

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        with mongo_context() as client:
            db = client[db_name]
            collection = db[collection_name]

            data['loaded_at'] = datetime.now()
            data['etl_version'] = '1.0.0'

            result = collection.insert_one(data)
            logger.info(f"Loaded predominant profile data. ID: {result.inserted_id}")
            return True
    except Exception as e:
        logger.error(f"Error loading predominant profile data: {e}")
        return False

def load_most_common_products(
        data: Dict[str, Any],
        db_name: str = 'DW-MarcosJunior',
        collection_name: str = 'ETL-most_common_products'
    ) -> bool:
    """
    Load the most common products bought together into MongoDB.

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        with mongo_context() as client:
            db = client[db_name]
            collection = db[collection_name]

            data['loaded_at'] = datetime.now()
            data['etl_version'] = '1.0.0'

            result = collection.insert_one(data)
            logger.info(f"Loaded most common products data. ID: {result.inserted_id}")
            return True
    except Exception as e:
        logger.error(f"Error loading most common products data: {e}")
        return False