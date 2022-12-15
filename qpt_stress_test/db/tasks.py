
import qpt_stress_test.core.config as config

import clickhouse_driver
import databases
import databricks.sql
import databricks.sql.client
import sqlalchemy
import pyodbc
import logging
from databases import DatabaseURL

logger = logging.getLogger(__name__)


def sv_awoh_dw01_pyodbc_connection_factory() -> pyodbc.Connection:
    """ """
    conn = pyodbc.connect(config.MSSQL_URL, autocommit=True)
    return conn


def bfc_rds_sqlalchemy_engine_factory() ->  sqlalchemy.engine.base.Engine:
    """ """
    engine = sqlalchemy.create_engine(str(config.POSTGRES_URL))
    return engine


async def bfc_rds_database_factory() -> databases.Database:
    """ """
    database = databases.Database(DatabaseURL(config.POSTGRES_URL), min_size=2, max_size=10)
    try:
        await database.connect()
    except Exception as e:
        logger.warning("--- DB CONNECTION ERROR ---")
        logger.warning(e)
        logger.warning("--- DB CONNECTION ERROR ---")

    return database


def gdt_cluster_databricks_connection_factory() -> databricks.sql.client.Connection:
    connection = databricks.sql.connect(
        server_hostname=config.DATABRICKS_SERVER_HOSTNAME,
        http_path=config.DATABRICKS_HTTP_PATH,
        access_token=config.DATABRICKS_ACCESS_TOKEN
    )
    return connection


def clickhouse_driver_client_factory() -> clickhouse_driver.client:
    client = clickhouse_driver.Client(
        host=config.CLICKHOUSE_HOST,
        user=config.CLICKHOUSE_USER,
        compression=False,
        send_receive_timeout=600,   # query must return in 10 minutes, max RAM is 10GB
        settings={
            'use_numpy': False, 
            'strings_encoding': 'ascii'
        }
    )
    return client
