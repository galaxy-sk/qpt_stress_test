
# -*- coding: utf-8 -*-
"""
"""
import datetime as dt

from .drivers.clickhouse import SqlQuery
from ..tasks import clickhouse_client
from qpt_stress_test.core.config import ChicagoTimeZone


GET_CME_MARK_PRICES = """
    with bar as (
        select name, max(`timestamp`) as mt
        from mark_prices.CME_future_1second 
        where `timestamp` BETWEEN '{0:%Y-%m-%d %H:%M:%S}' and '{1:%Y-%m-%d %H:%M:%S}'
        group by name
    )
    select name, price
    from mark_prices.CME_future_1second t1
        inner join bar on bar.name = t1.name and bar.mt = t1.`timestamp` 
    order by t1.name asc;
"""
#min_time, max_time

GET_LAST_TRADING_BALANCES_EOD_TRADEDATE = "SELECT Max(TradeDate) as last_date FROM qpt.trading_balances_eod txn;"

class TradingRepository:

    def __init__(self):
        pass
    
    def adhoc_query(self, sql: str) -> SqlQuery:
        return SqlQuery(sql, databricks_connection_fn=clickhouse_client)

    @property
    def last_positions_date(self) -> dt.date:
        _, data = SqlQuery(GET_LAST_TRADING_BALANCES_EOD_TRADEDATE, databricks_connection_fn=clickhouse_client).as_list()
        return data[0][0].date()

    def get_cme_positions(self, at_dtt_utc: dt.datetime) -> SqlQuery:
        # maybe work out from trade list when we actually started tradeing this instrument
        return SqlQuery(GET_CME_MARK_PRICES, 
                        at_dtt_utc.astimezone(ChicagoTimeZone).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                        at_dtt_utc.date().strftime('%Y-%m-%d'),
                        databricks_connection_fn=clickhouse_client)
