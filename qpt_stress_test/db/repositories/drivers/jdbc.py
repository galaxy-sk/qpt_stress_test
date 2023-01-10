
from qpt_stress_test.core.config import POSTGRES_URL, POSTGRES_JDBC_DRIVER, MSSQL_URL, MSSQL_JDBC_DRIVER, MSSQL_USER, MSSQL_PASSWORD

import logging
from collections.abc import Callable

from .interfaces import SqlQueryInterface


logger = logging.getLogger(__name__)


class DatabricksSqlQuery(SqlQueryInterface):

    def __init__(self, query: str, *params, db_connector_factory=None, spark_session=None):
        self._query = query.format(*params)
        self._params = params
        self._jdbc_connector_factory = db_connector_factory
        self._spark_session = spark_session
        
    def as_dataframe(self):
        df = self._spark_session.sql(self._query)
        return df

    def as_instrument_map(self) -> dict:
        with self.db_cursor() as cursor:
            cursor.execute(self._query)
            rs = cursor.fetchall()
            column_names = [column[0] for column in cursor.description]
            map = {
                (row.instrument if "instrument" in column_names else row.symbol_bfc).upper(): {
                    column[0]: value
                    for column, value in zip(cursor.description, row)
                }
                for row in rs
            }     
        return map

    def as_map(self) -> dict:
        with self.db_cursor() as cursor:
            cursor.execute(self._query)
            rs = cursor.fetchall()
            map = {
                idx: {
                    column[0]: value
                    for column, value in zip(cursor.description, row)
                }
                for idx, row in enumerate(rs)
            }
        return map

    def as_list(self) -> tuple:
        with self.db_cursor() as cursor:
            cursor.execute(self._query)
            rs = cursor.fetchall()
            column_names = [column[0] for column in cursor.description]
            data = [[value for value in row] for row in rs]
        return column_names, data


class MsSqlQuery:
    # jdbc:sqlserver://10.17.181.234:1433;encrypt=true;trustServerCertificate=true;database=Trading; 
    # com.microsoft.sqlserver.jdbc.SQLServerDriver
    def __init__(self, query: str, *params, spark_session=None, db_connector_factory=None):
        self._query = query.format(*params)
        self._params = params
        self._jdbc_connector_factory = db_connector_factory
        self._spark_session = spark_session
        
    def as_dataframe(self):
        df = self._spark_session.read \
            .format("jdbc") \
            .option("url", MSSQL_URL) \
            .option("driver", MSSQL_JDBC_DRIVER) \
            .option("dbtable", self._query) \
            .load()
        return df

    def as_instrument_map(self) -> dict:
        with self.db_cursor() as cursor:
            cursor.execute(self._query)
            rs = cursor.fetchall()
            column_names = [column[0] for column in cursor.description]
            map = {
                (row.instrument if "instrument" in column_names else row.symbol_bfc).upper(): {
                    column[0]: value
                    for column, value in zip(cursor.description, row)
                }
                for row in rs
            }     
        return map

    def as_list(self) -> tuple:
        with self.db_cursor() as cursor:
            cursor.execute(self._query)
            rs = cursor.fetchall()
            column_names = [column[0] for column in cursor.description]
            data = [[value for value in row] for row in rs]
        return column_names, data


class SparkJdbcConnector:
    
    @staticmethod
    def qpt_mssql_connector(spark_session):
        return SparkJdbcConnector(spark_session, MSSQL_URL, MSSQL_JDBC_DRIVER, MSSQL_USER, MSSQL_PASSWORD)

    @staticmethod
    def qpt_pg_connector(spark_session):
        return SparkJdbcConnector(spark_session, POSTGRES_URL, POSTGRES_JDBC_DRIVER)

    def __init__(self, spark_session, url, driver, user=None, password=None):
        self._spark_session = spark_session
        self._url = url
        self._driver = driver
        self._user = user
        self._password = password
    
    @property
    def read(self):
        conn = self._spark_session.read \
            .format("jdbc") \
            .option("url", self._url) \
            .option("driver", self._driver)
        
        # MSSQL requires user and password options
        if self._user:
            conn = conn.option("user",  self._user)
        if self._password:
            conn = conn.option("password", self._password)
        return conn


class SqlQuery:
    # jdbc:postgresql://bfc-rds-2.cq7gi4betiom.us-east-2.rds.amazonaws.com:5432/trading?user=xxx&password=xxx
    #  org.postgresql.Driver

    def __init__(self, query: str, *params, db_connector_factory=None):
        self._raw_query = query
        self._params = params
        self._query = query.format(*params)
        self._db_connector_factory = db_connector_factory
        
    def as_dataframe(self):
        df = self._db_connector_factory.read \
            .option("query", self._query) \
            .load()
        return df

    def as_instrument_map(self) -> dict:
        df = self.as_dataframe()
        for idx, row in df.rowiter():
            column_names =  [column.name for column in rs.cursor.description]
            map = {row.instrument if "instrument" in column_names else row.symbol_bfc: row for row in rs}
        return map

    def as_list(self) -> tuple:
        df = self.as_dataframe()
        for idx, row in df.rowiter():
            column_names = [column.name for column in rs.cursor.description]
            data = [[value for value in row] for row in rs]
        return column_names, data
