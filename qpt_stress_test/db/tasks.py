
from qpt_stress_test.core.config import POSTGRES_URL, MSSQL_URL

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
    conn = pyodbc.connect(MSSQL_URL,  autocommit=True)
    return conn


def bfc_rds_pg_engine() ->  sqlalchemy.engine.base.Engine:
    # import psycopg2 as pg
    # report_dt = dt.datetime(2022, 10, 23, 21, 0, 0)
    engine = sqlalchemy.create_engine(str(POSTGRES_URL))
    return engine


async def pgsql_database() -> databases.Database:
    # import psycopg2 as pg
    # connection = pg.connect(f"host={config.pg_db_host} dbname={config.pg_db_name} user={config.pg_db_user} password={config.pg_db_password}")
    database = databases.Database(POSTGRES_URL, min_size=2, max_size=10)  # these can be configured in config as well
    try:
        await database.connect()
    except Exception as e:
        logger.warning("--- DB CONNECTION ERROR ---")
        logger.warning(e)
        logger.warning("--- DB CONNECTION ERROR ---")

    return database


def  gdt_cluster_connection() -> databricks.sql.client.Connection:
    return databricks.sql.connect(
        server_hostname = "gdt-mo.cloud.databricks.com",            #os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        http_path       = "/sql/1.0/endpoints/b09bc4bc73bccd24",    #os.getenv("DATABRICKS_HTTP_PATH"),
        access_token    = "dapi11e376e20349a386f4764df7af20504a")   #os.getenv("DATABRICKS_TOKEN")
