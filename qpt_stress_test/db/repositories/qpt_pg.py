# -*- coding: utf-8 -*-
"""

"""
import datetime as dt

from .drivers.sqlalchemy import SqlQuery
from ..tasks import bfc_rds_pg_engine

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
GET_LAST_TRADING_BALANCES_EOD_TRADEDATE = "SELECT Max(snapshot_time) as last_snapshot_time FROM rms.exchange_position_snapshot;"

#snapshot_time, 
#    t1.long_positions_notional, t1.short_positions_notional, t1.long_positions, t1.short_positions,
#    t1.mark_prices, t1.long_unrealized_pnls, t1.short_unrealized_pnls
#    from rms.exchange_position_snapshot t1

class TradingRepository:

    def __init__(self):
        pass
    
    def adhoc_query(self, sql: str) -> SqlQuery:
        return SqlQuery(sql, sqlalchemy_engine_fn=bfc_rds_pg_engine)

    @property
    def last_positions_date(self) -> dt.date:
        _, data = SqlQuery(GET_LAST_TRADING_BALANCES_EOD_TRADEDATE, sqlalchemy_engine_fn=bfc_rds_pg_engine).as_list()
        return data[0][0].date()

    def get_spot_definitions(self) -> SqlQuery:
        # maybe work out from trade list when we actually started tradeing this instrument 
        return SqlQuery(GET_SPOT_CONTRACTS, sqlalchemy_engine_fn=bfc_rds_pg_engine)


# Table columns:
# 'symbol_bfc', 'symbol_exch', 'exchange', 'endpoint', 'instrument_type',
# 'expiration_time', 'qty_multiplier', 'currency_position', 'currency_quote', 'currency_settlement', 'is_linear',
# 'underlying', 'id', 'symbol_root', 'currency_index', 'future_type', 'gateway_security_type',
# ''is_active', contract_size', 'tick_size', 'maximal_order_qty',
# 'ext_val1_name', 'ext_val1', 'ext_val2_name', 'ext_val2', 'ext_val3_name', 'ext_val3', 'updated_time_cst',
# 'inserted_time_cst', 'dma_tick', 'exchange_id',
# 'strike_price', 'raw_content'

GET_ACTIVE_CONTRACTS = """
    select 
       symbol_bfc, 
       symbol_exch,
       upper(exchange) as exchange, 
       endpoint,
       instrument_type, 
       expiration_time, 
       is_linear, 
       contract_size, 
       qty_multiplier, 
       currency_position,
       currency_settlement, 
       case when is_linear > 0 then currency_position else currency_settlement end as underlying
    from 
       Trading.definition.instrument_reference 
    where 
       instrument_type in ('SWAP', 'FUTURE') and symbol_bfc is not null and expiration_time >= '{0:%Y-%m-%d 00:00:00.000}' 
"""

GET_SPOT_CONTRACTS = """
    select 
       symbol_bfc, 
       symbol_exch,
       upper(exchange) as exchange, 
       endpoint,
       instrument_type, 
       qty_multiplier, 
       currency_position
    from 
       Trading.definition.instrument_reference 
    where 
       instrument_type not in ('SWAP', 'FUTURE') AND symbol_bfc is not null 
"""

GET_CONTRACT_DETAILS = """
    select 
       symbol_bfc, 
       symbol_exch,
       upper(exchange) as exchange, 
       endpoint,
       instrument_type, 
       expiration_time,
       is_linear, 
       contract_size, 
       qty_multiplier, 
       currency_position,
       currency_settlement, 
       case when is_linear > 0 then currency_position else currency_settlement end as underlying,
       symbol_root, 
       currency_index
    from 
       Trading.definition.instrument_reference 
    where 
       is_active = 1 and symbol_bfc in {symbols};
"""

class ReferenceRepository:

    def get_active_contracts(self, from_dt: dt.date) -> SqlQuery:
        # maybe work out from trade list when we actually started tradeing this instrument 
        return SqlQuery(GET_ACTIVE_CONTRACTS, from_dt, sqlalchemy_engine_fn=bfc_rds_pg_engine)

    def get_instruments_details(self, symbol_bfcs: list) -> SqlQuery:
        return SqlQuery(GET_CONTRACT_DETAILS.format(symbols=tuple(symbol_bfcs)), sqlalchemy_engine_fn=bfc_rds_pg_engine)
