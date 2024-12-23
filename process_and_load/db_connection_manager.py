""""
Database connection pool and conn ops functions

Based on Psycopg3 Binary Package
To install and create layer on AWS Lambda function use this command.
-> pip install --target ./python --platform manylinux2014_x86_64 --implementation cp --only-binary=:all: --upgrade "psycopg[pool,binary]"
"""

import psycopg_pool
import os

# Configurations of Connection Pool
if 'DB_CONN_POOL_MIN_SIZE' in os.environ:
    MIN_SIZE = int(os.environ['DB_CONN_POOL_MIN_SIZE'])
else:
    MIN_SIZE = 1

if 'DB_CONN_POOL_MAX_SIZE' in os.environ:
    MAX_SIZE = int(os.environ['DB_CONN_POOL_MAX_SIZE'])
else:
    MAX_SIZE = 5

if 'DB_GET_CONN_TIMEOUT' in os.environ:
    GET_CONN_TIMEOUT = int(os.environ['DB_GET_CONN_TIMEOUT'])
else:
    GET_CONN_TIMEOUT = 30

CONN_STRING = """
    dbname=postgres
    user=postgres
    password=arvindbondkar
    host=database-test.cdvveo1lemjv.ap-south-1.rds.amazonaws.com
    port=5432
    """


# Creating connection pool. Lambda reuses this when warm-starts
_DB_CONNECTION_POOL = psycopg_pool.ConnectionPool(
    # environ setting in lambda to set min & max connections of pool
    min_size=MIN_SIZE, max_size=MAX_SIZE,

    conninfo=CONN_STRING,
    open=True,
    timeout=GET_CONN_TIMEOUT
)


def get_db_connection():
    """
    Gets a connection from connection pool
    :return:
    """
    try:
        conn = _DB_CONNECTION_POOL.getconn()
        return conn
    except Exception as e:
        raise Exception(f"getConn() from Pool failed{e}")


def put_db_connection(conn):
    """
    Puts connection back into connection pool
    :param conn:
    :return:
    """
    try:
        _DB_CONNECTION_POOL.putconn(conn)
        return True
    except Exception as e:
        raise Exception(f"putConn() into Pool failed{e}")

