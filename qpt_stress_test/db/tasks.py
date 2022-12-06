
import qpt_stress_test.core.config as config

import clickhouse_driver
import databases
import databricks.sql
import databricks.sql.client
import sqlalchemy
import pyodbc
import logging

logger = logging.getLogger(__name__)


def qpt_sqlsvr_connection() -> pyodbc.Connection:
    # pyodbc locks the schema when `autocommit=False`
    # conn = pyodbc.connect(driver_addr, autocommit=False)
    conn = pyodbc.connect(config.MSSQL_URL, autocommit=True)
    return conn


def bfc_rds_pg_engine() ->  sqlalchemy.engine.base.Engine:
    # import psycopg2 as pg
    # report_dt = dt.datetime(2022, 10, 23, 21, 0, 0)
    engine = sqlalchemy.create_engine(str(config.POSTGRES_URL))
    return engine


async def pgsql_database() -> databases.Database:
    # import psycopg2 as pg
    # connection = pg.connect(f"host={config.pg_db_host} dbname={config.pg_db_name} user={config.pg_db_user} password={config.pg_db_password}")
    database = databases.Database(config.POSTGRES_URL, min_size=2, max_size=10)  # these can be configured in config as well
    try:
        await database.connect()
    except Exception as e:
        logger.warning("--- DB CONNECTION ERROR ---")
        logger.warning(e)
        logger.warning("--- DB CONNECTION ERROR ---")

    return database


def gdt_cluster_connection() -> databricks.sql.client.Connection:
    connection = databricks.sql.connect(
        server_hostname = config.DATABRICKS_SERVER_HOSTNAME,   # "gdt-mo.cloud.databricks.com",            #os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        http_path = config.DATABRICKS_HTTP_PATH,        # "/sql/1.0/endpoints/b09bc4bc73bccd24",    #os.getenv("DATABRICKS_HTTP_PATH"),
        access_token = config.DATABRICKS_ACCESS_TOKEN   # "dapi11e376e20349a386f4764df7af20504a")   #os.getenv("DATABRICKS_TOKEN")
    )
    return connection


def clickhouse_client() -> clickhouse_driver.client:
    client = clickhouse_driver.Client(
        host = config.CLICKHOUSE_HOST, # '10.17.181.234'  # 'sv-awoh-md1.na.bluefirecap.net'
        user = config.CLICKHOUSE_USER, # 'reader1', 
        compression = False, 
        send_receive_timeout = 600,   # query must return in 10 minutes, max RAM is 10GB
        settings = {
            'use_numpy': False, 
            'strings_encoding': 'ascii'
        }
    )
    return client
