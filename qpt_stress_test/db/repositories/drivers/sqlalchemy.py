
import logging
import pandas as pd
import sqlalchemy

from collections.abc import Callable
from contextlib import ContextDecorator, contextmanager


logger = logging.getLogger(__name__)


class SqlQuery:

    @contextmanager
    def db_cursor(*args, **kwargs):
        with args[0]._sqlalchemy_engine_fn().connect() as connection:
            try:
                yield connection
            finally:
                pass
    
    def __init__(self, query: str, *params, sqlalchemy_engine_fn: Callable[[], sqlalchemy.engine.base.Engine]=None):
        self._raw_query = query
        self._params = params
        self._query = query.format(*params)
        self._sqlalchemy_engine_fn = sqlalchemy_engine_fn

    def as_dataframe(self):
        df = pd.read_sql(self._query, con=self._sqlalchemy_engine_fn())
        return df

    def as_instrument_map(self) -> dict:
        with self.db_cursor() as con:
            rs = con.execute(self._query)
            map = {
                row.instrument: row 
                for row in rs
            }
        return map

    def as_map(self) -> dict:
        with self.db_cursor() as con:
            rs = con.execute(self._query)
            map = {
                idx: row 
                for idx, row in enumerate(rs)
            }
        return map

    def as_list(self) -> tuple:
        with self.db_cursor() as con:
            rs = con.execute(self._query)
            columns = [column.name for column in rs.cursor.description]
            data = [[value for value in row] for row in rs]
        return columns, data
