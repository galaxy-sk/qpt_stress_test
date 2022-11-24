# -*- coding: utf-8 -*-
"""

"""
import datetime as dt

from .drivers.sqlalchemy import SqlQuery
from ..tasks import bfc_rds_pg_engine

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
       symbol_bfc is not null and expiration_time >= '{0:%Y-%m-%d 00:00:00.000}' 
    limit 10000
"""


class TradingRepository:

    def __init__(self):
        pass
    
    def get_active_contracts(self, from_dt: dt.date, to_dt: dt.date):
        # maybe work out from trade list when we actually started tradeing this instrument 
        return SqlQuery(GET_ACTIVE_CRYPTO_CONTRACTS, from_dt, sqlalchemy_engine_fn=bfc_rds_pg_engine)


"""pg_conn = db_tasks.bfc_rds_pg_engine()
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
