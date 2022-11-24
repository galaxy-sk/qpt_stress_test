
import logging
import pandas as pd
import pyodbc

from collections.abc import Callable
from contextlib import ContextDecorator, contextmanager

logger = logging.getLogger(__name__)


class SqlQuery:

    @contextmanager
    def db_cursor(self, *args, **kwargs):
        cursor = self._pyodbc_conn_fn().cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    class cursor_context(ContextDecorator):
        def __init__(self):
            self.cursor = None

        def __enter__(self, *args, **kwargs):
            self.cursor = args[0]._pyodbc_conn_fn().cursor()
            return self

        def __call__(self, func):
            super().__call__(func)
            
        def __exit__(self, *exc):
            self.cursor.close()
            return False

    def __init__(self, query: str, *params, pyodbc_conn_fn: Callable[[], pyodbc.Connection] = None):
        self._query = query.format(*params)
        self._params = params
        self._pyodbc_conn_fn = pyodbc_conn_fn


    def as_dataframe(self):
        df = pd.read_sql(self._query, con=self._pyodbc_conn_fn())
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
                str(hash(tuple(row))): {
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
