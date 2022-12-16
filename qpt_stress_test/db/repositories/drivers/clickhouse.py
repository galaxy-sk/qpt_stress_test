
import pandas as pd

import clickhouse_driver
from collections.abc import Callable
from contextlib import ContextDecorator, contextmanager

from .interfaces import SqlQueryInterface


class SqlQuery(SqlQueryInterface):

    @contextmanager
    def db_cursor(self, *args, **kwargs):
        with self._databricks_connection_fn() as connection:
            with connection.cursor() as cursor:
                yield cursor

    class cursor_context(ContextDecorator):
        def __init__(self):
            self.cursor = None

        def __enter__(self, *args, **kwargs):
            self.cursor = args[0]._databricks_connection_fn().cursor()
            return self

        def __call__(self, func):
            super().__call__(func)
            
        def __exit__(self, *exc):
            self.cursor.close()
            return False

    def __init__(self, query: str, *params, databricks_connection_fn: Callable[[], databricks.sql.client.Connection] = None):
        self._query = query.format(*params)
        self._params = params
        self._databricks_connection_fn = databricks_connection_fn


    def as_dataframe(self):
        df = pd.read_sql(self._query, con=self._databricks_connection_fn())
        return df

    def as_instrument_map(self) -> dict:
        with self.db_cursor() as cursor:
            cursor.execute(self._query)
            rs = cursor.fetchall()
            map = {
                (row.instrument if "instrument" in row else row.symbol_bfc).upper(): {
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
