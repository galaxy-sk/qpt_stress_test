
from qpt_stress_test.core.config import POSTGRES_URL, POSTGRES_JDBC_DRIVER, MSSQL_URL, MSSQL_JDBC_DRIVER

import logging
from collections.abc import Callable

logger = logging.getLogger(__name__)


class DatabricksSqlQuery:

    def __init__(self, query: str, *params, db_conn=None, spark_session=None):
        self._query = query.format(*params)
        self._params = params
        self._db_conn = db_conn
        self._spark_session = spark_session
        
    def as_dataframe(self):
        df = self._spark_session.sql(self._query)
        return df

    def as_instrument_map(self) -> dict:
        with self.db_cursor() as cursor:
            cursor.execute(self._query)
            rs = cursor.fetchall()
            map = {
                row.instrument.upper(): {
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
            columns = [column[0] for column in cursor.description]
            data = [[value for value in row] for row in rs]
        return columns, data


class MsSqlQuery:
    # jdbc:sqlserver://10.17.181.234:1433;encrypt=true;trustServerCertificate=true;database=Trading; 
    # com.microsoft.sqlserver.jdbc.SQLServerDriver
    def __init__(self, query: str, *params, spark_session=None, db_conn=None):
        self._query = query.format(*params)
        self._params = params
        self._db_conn = db_conn
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
            map = {
                row.instrument.upper(): {
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
            columns = [column[0] for column in cursor.description]
            data = [[value for value in row] for row in rs]
        return columns, data


class PgSqlQuery:
    # jdbc:postgresql://bfc-rds-2.cq7gi4betiom.us-east-2.rds.amazonaws.com:5432/trading?user=xxx&password=xxx
    #  org.postgresql.Driver

    def __init__(self, query: str, *params, spark_session=None, db_conn=None):
        self._raw_query = query
        self._params = params
        self._query = query.format(*params)
        self._spark_session = spark_session
        
    def as_dataframe(self):
        df = self._spark_session.read \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("driver", POSTGRES_JDBC_DRIVER) \
            .option("dbtable", exchange_position_snapshot_sql) \
            .load()
        return df

    def as_instrument_map(self) -> dict:
        with self.db_cursor() as con:
            rs = con.execute(self._query)
            map = {row.instrument: row for row in rs}
        return map

    def as_list(self) -> tuple:
        with self.db_cursor() as con:
            rs = con.execute(self._query)
            columns = [column.name for column in rs.cursor.description]
            data = [[value for value in row] for row in rs]
        return columns, data
