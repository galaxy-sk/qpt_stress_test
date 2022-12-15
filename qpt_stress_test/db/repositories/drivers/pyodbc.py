
import logging
import pandas as pd
import pyodbc

from collections.abc import Callable
from contextlib import ContextDecorator, contextmanager

logger = logging.getLogger(__name__)


class SqlQuery:
    """ Class assumes connection factory returns a pyodbc.Connection object """
    
    @contextmanager
    def db_cursor(self, *args, **kwargs):
        cursor = self._pyodbc_conn_factory().cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    class cursor_context(ContextDecorator):
        def __init__(self):
            self.cursor = None

        def __enter__(self, *args, **kwargs):
            self.cursor = args[0]._pyodbc_conn_factory().cursor()
            return self

        def __call__(self, func):
            super().__call__(func)
            
        def __exit__(self, *exc):
            self.cursor.close()
            return False

    def __init__(self, query: str, *params, db_connector_factory: Callable[[], pyodbc.Connection] = None):
        self._query = query.format(*params)
        self._params = params
        self._pyodbc_conn_factory = db_connector_factory

    def as_dataframe(self):
        df = pd.read_sql(self._query, con=self._pyodbc_conn_factory())
        return df

    def as_instrument_map(self) -> dict:
        with self.db_cursor() as cursor:
            cursor.execute(self._query)
            rs = cursor.fetchall()
            column_names = [col[0] for col in cursor.description]
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
            rs_as_map = {
                idx: {
                    column[0]: value
                    for column, value in zip(cursor.description, row)
                }
                for idx, row in enumerate(rs)
            }     
        return rs_as_map

    def as_list(self) -> tuple:
        with self.db_cursor() as cursor:
            cursor.execute(self._query)
            rs = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            data = [[value for value in row] for row in rs]
        return columns, data
