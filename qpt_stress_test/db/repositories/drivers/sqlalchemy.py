
import logging
import pandas as pd
import sqlalchemy

from collections.abc import Callable
from contextlib import ContextDecorator, contextmanager


logger = logging.getLogger(__name__)


class SqlQuery:
    """ Class assumes db_connector_factory() produces a sqlalchemy.engine.base.Engine object """

    @contextmanager
    def db_cursor(*args, **kwargs):
        with args[0]._sqlalchemy_engine_factory().connect() as connection:
            try:
                yield connection
            finally:
                pass
    
    def __init__(self, query: str, *params, db_connector_factory: Callable[[], sqlalchemy.engine.base.Engine]=None):
        self._raw_query = query
        self._params = params
        self._query = query.format(*params)
        self._sqlalchemy_engine_factory = db_connector_factory

    def as_dataframe(self):
        df = pd.read_sql(self._query, con=self._sqlalchemy_engine_factory())
        return df

    def as_instrument_map(self) -> dict:
        with self.db_cursor() as con:
            rs = con.execute(self._query)
            column_names = [column.name for column in rs.cursor.description]
            map = {
                row.instrument if "instrument" in column_names else row.symbol_bfc: row 
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
            column_names = [column.name for column in rs.cursor.description]
            data = [[value for value in row] for row in rs]
        return column_names, data
