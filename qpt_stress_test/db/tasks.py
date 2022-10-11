
from qpt_stress_test.core.config import POSTGRES_URL, MSSQL_URL

from databases import Database
import sqlalchemy
import pyodbc
import logging

logger = logging.getLogger(__name__)


def ms_conn():
    # pyodbc locks the schema when `autocommit=False`
    # conn = pyodbc.connect(driver_addr, autocommit=False)
    conn = pyodbc.connect(MSSQL_URL,  autocommit=True)
    return conn

def pgsql_engine() ->  sqlalchemy.engine.base.Engine:
    # import psycopg2 as pg
    # report_dt = dt.datetime(2022, 10, 23, 21, 0, 0)
    engine = sqlalchemy.create_engine(str(POSTGRES_URL))
    return engine

async def pgsql_connection() -> Database:
    # import psycopg2 as pg
    # connection = pg.connect(f"host={config.pg_db_host} dbname={config.pg_db_name} user={config.pg_db_user} password={config.pg_db_password}")


    database = Database(POSTGRES_URL, min_size=2, max_size=10)  # these can be configured in config as well
    try:
        await database.connect()
    except Exception as e:
            logger.warning("--- DB CONNECTION ERROR ---")
            logger.warning(e)
            logger.warning("--- DB CONNECTION ERROR ---")

    return database
