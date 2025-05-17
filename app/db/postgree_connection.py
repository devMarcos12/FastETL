import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from contextlib import contextmanager
import os

logger = logging.getLogger(__name__)

class PostgresConnection:   
    _instance = None
    _connection = None

    @classmethod
    def get_connection(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._connection
    
    def __init__(self):
        if PostgresConnection._instance is not None:
            raise Exception("This class is a singleton!")
        else:
            PostgresConnection._instance = self
            self._connect()
    
    def _connect(self):
        load_dotenv()
        try:
            PostgresConnection._connection = psycopg2.connect(
                dbname=os.getenv('POSTGRES_DB'),
                user=os.getenv('POSTGRES_USER'),
                password=os.getenv('POSTGRES_PASSWORD'),
                host=os.getenv('POSTGRES_HOST'),
                port=os.getenv('POSTGRES_PORT')
            )
            logger.info("Connection to PostgreSQL established.")
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            raise

    @classmethod
    def close_connection(cls):
        if cls._connection:
            cls._connection.close()
            cls._connection = None
            cls._instance = None
            logger.info("Connection to PostgreSQL closed.")

@contextmanager
def postgres_cursor(cursor_factory=None):
    """Context manager for using a PostgreSQL cursor."""
    conn = get_postgres_connection()
    cursor = None
    try:
        if cursor_factory:
            cursor = conn.cursor(cursor_factory=cursor_factory)
        else:
            cursor = conn.cursor()
        yield cursor
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        if cursor:
            cursor.close()

@contextmanager
def dict_cursor():
    """Context manager for using a PostgreSQL cursor with RealDictCursor."""
    with postgres_cursor(RealDictCursor) as cursor:
        yield cursor

def get_postgres_connection():
    return PostgresConnection.get_connection()

def get_new_postgres_connection():
    """Create a new PostgreSQL connection without using the singleton."""
    load_dotenv()
    try:
        connection = psycopg2.connect(
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT')
        )
        logger.info("New PostgreSQL connection created.")
        return connection
    except Exception as e:
        logger.error(f"Error creating new PostgreSQL connection: {e}")
        raise