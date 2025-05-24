import logging
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
from contextlib import contextmanager
import os

logger = logging.getLogger(__name__)

class MongoDBConnection:
    _instance = None
    _client = None

    @classmethod
    def get_client(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._client

    def __init__(self):
        if MongoDBConnection._instance is not None:
            raise Exception("This class is a singleton!")
        else:
            MongoDBConnection._instance = self
            self._connect()

    def _connect(self):
        load_dotenv()
        uri = os.getenv('MONGODB_URI')

        if not uri:
            logger.error("MONGODB_URI not found in environment variables")
            raise ValueError("MONGODB_URI not found in environment variables")

        try:
            MongoDBConnection._client = MongoClient(uri, server_api=ServerApi('1'))
            MongoDBConnection._client.admin.command('ping')
            logger.info("Connection to MongoDB established successfully.")
        except Exception as e:
            logger.error(f"Error connecting to MongoDB: {e}")
            raise

    @classmethod
    def close_connection(cls):
        if cls._client:
            cls._client.close()
            cls._client = None
            cls._instance = None
            logger.info("Connection to MongoDB closed.")

@contextmanager
def mongo_context():
    """Context manager for safe MongoDB connection handling."""
    client = None
    try:
        client = get_mongo_connection()
        yield client
    finally:
        pass

def get_mongo_connection():
    return MongoDBConnection.get_client()