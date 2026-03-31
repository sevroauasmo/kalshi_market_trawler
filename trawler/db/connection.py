import logging

import psycopg2

from trawler.config import DATABASE_URL

log = logging.getLogger(__name__)


def get_connection():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    return conn
