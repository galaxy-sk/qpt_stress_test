# -*- coding: utf-8 -*-
"""

"""
import datetime as dt
import pandas as pd
from contextlib import ContextDecorator, contextmanager


from ..tasks import pgsql_engine




class SqlQuery:

    @contextmanager
    def db_cursor(*args, **kwargs):
        with args[0]._db_conn.connect() as connection:
            try:
                yield connection
            finally:
                pass
    def __init__(self, query: str, *params, db_conn=pgsql_engine()):
        self._raw_query = query
        self._params = params
        self._query = query.format(*params)
        self._db_conn = db_conn

    def as_dataframe(self):
        df = pd.read_sql(self._query, con=self._db_conn)
        return df

    def as_map(self) -> dict:
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


GET_ACTIVE_CRYPTO_CONTRACTS = """
    select 
       symbol_bfc as instrument, 
       upper(exchange) as exchange, 
       symbol_exch,
       endpoint, 
       instrument_type, 
       expiration_time, 
       is_linear,  
       contract_size, 
       qty_multiplier, 
       case when is_linear > 0 then currency_position else currency_settlement end as underlying 
    from 
       Trading.definition.instrument_reference 
    where 
       symbol_bfc is not null and expiration_time >= '{}' 
    limit 10000
"""


class TradingRepository:

    def __init__(self, db_conn=pgsql_engine()):
        self._db_conn = db_conn
    
    def get_active_contracts(self, from_dt: dt.date, to_dt: dt.date):
        # maybe work out from trade list when we actually started tradeing this instrument 
        return SqlQuery(GET_ACTIVE_CRYPTO_CONTRACTS, f"{from_dt:%Y-%m-%d 00:00:00.000}")


"""pg_conn = db_tasks.pgsql_engine()
sql = (
        "select "
        "   symbol_bfc as instrument, "
        "   upper(exchange) as exchange, "
        "   symbol as exchange_symbol, "
        "   instrument_type, "
        "   expiration_time, "
        "   is_linear,  "
        "   case when is_linear > 0 then currency_position else currency_settlement end as underlying "
        "from "
        "   Trading.definition.instrument_reference "
        "where "
        "   symbol_bfc is not null and expiration_time >= '{}' "
        "limit 5000").format(reporting_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
df = pd.read_sql(sql, con=pg_conn)
"""
