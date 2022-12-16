# -*- coding: utf-8 -*-
"""

"""
import datetime as dt

from .drivers.interfaces import SqlQueryInterface

"""pg_conn = db_tasks.bfc_rds_sqlalchemy_engine_factory()
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

GET_NET_OPEN_POSITIONS_SNAPSHOT = """
    select t1.exchange, t1.account, t1.snapshot_time, t1.long_positions_notional, t1.short_positions_notional, t1.long_positions, t1.short_positions, t1.mark_prices, t1.long_unrealized_pnls, t1.short_unrealized_pnls
    from rms.exchange_position_snapshot t1
        inner join (
            select account, min(snapshot_time) as min_time
            from rms.exchange_position_snapshot
            where snapshot_time between '{asof_start:%Y-%m-%d %H:%M:%S.000}' and '{asof_end:%Y-%m-%d %H:%M:%S.000}'
            group by account) t2 on t1.account = t2.account and t1.snapshot_time = t2.min_time
"""

#snapshot_time, 
#    t1.long_positions_notional, t1.short_positions_notional, t1.long_positions, t1.short_positions,
#    t1.mark_prices, t1.long_unrealized_pnls, t1.short_unrealized_pnls
#    from rms.exchange_position_snapshot t1

class TradingRepository:

    def __init__(self, sql_query_driver, db_connector_factory):
        self._sql_query_class = sql_query_driver
        self._db_connector_factory = db_connector_factory
    
    def adhoc_query(self, sql: str) -> SqlQueryInterface:
        return self._sql_query_class(
            sql, 
            db_connector_factory=self._db_connector_factory)

    @property
    def last_positions_date(self) -> dt.date:
        _, data = self._sql_query_class(
            GET_LAST_TRADING_BALANCES_EOD_TRADEDATE, 
            db_connector_factory=self._db_connector_factory).as_list()
        return data[0][0].date()

    def get_spot_definitions(self) -> SqlQueryInterface:
        # maybe work out from trade list when we actually started tradeing this instrument 
        return self._sql_query_class(
            GET_SPOT_CONTRACTS, 
            db_connector_factory=self._db_connector_factory)


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

    def __init__(self, sql_query_driver, db_connector_factory):
        self._sql_query_class = sql_query_driver
        self._db_connector_factory = db_connector_factory
    
    def get_active_contracts(self, from_dt: dt.date) -> SqlQueryInterface:
        # maybe work out from trade list when we actually started tradeing this instrument 
        return self._sql_query_class(
            GET_ACTIVE_CONTRACTS, from_dt, 
            db_connector_factory=self._db_connector_factory)

    def get_instruments_details(self, symbol_bfcs: list) -> SqlQueryInterface:
        return self._sql_query_class(
            GET_CONTRACT_DETAILS.format(symbols=tuple(symbol_bfcs)), 
            db_connector_factory=self._db_connector_factory)
