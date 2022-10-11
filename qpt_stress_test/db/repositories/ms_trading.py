# -*- coding: utf-8 -*-
"""
"""
import datetime as dt
import pandas as pd
from contextlib import ContextDecorator, contextmanager

from ..tasks import ms_conn
from qpt_stress_test.core.config import ChicagoTimeZone


class SqlQuery:

    @contextmanager
    def db_cursor(self, *args, **kwargs):
        cursor = self._db_conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    class cursor_context(ContextDecorator):
        def __init__(self):
            self.cursor = None

        def __enter__(self, *args, **kwargs):
            self.cursor = args[0]._db_conn.cursor()
            return self

        def __call__(self, func):
            super().__call__(func)
            
        def __exit__(self, *exc):
            self.cursor.close()
            return False

    def __init__(self, query: str, *params, db_conn=ms_conn()):
        self._query = query.format(*params)
        self._params = params
        self._db_conn = db_conn


    def as_dataframe(self):
        df = pd.read_sql(self._query, con=self._db_conn)
        return df

    def as_map(self) -> dict:
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

    def as_list(self) -> tuple:
        with self.db_cursor() as cursor:
            cursor.execute(self._query)
            rs = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            data = [[value for value in row] for row in rs]
        return columns, data


GET_CME_POSITIONS = """
    WITH Symbols([Contract], Exchange, Multiplier, SecurityType, Expiration, CurrencyCode) AS (
		SELECT [Contract], Exchange, Multiplier, SecurityType, Expiration, CurrencyCode
		FROM Trading.dbo.products WITH(NOLOCK)
		WHERE Exchange = 'CME'
		AND SecurityType = 'FUTURE'
		AND Expiration > CONVERT(DATETIME, '{0}')),

	Marks(Symbol, Exchange, TradeDate, SettlementPrice, AsOf) AS (
		SELECT Symbol, Exchange, TradeDate, SettlementPrice, AsOf
		FROM trading.crypto.CMEOfficialSettlement WITH(NOLOCK)
		WHERE Symbol in (SELECT [Contract] FROM Symbols) AND TradeDate = CONVERT(DATE, '{1}')),

	Positions(Symbol, Account, NetQuantity, LongQuantity, ShortQuantity, LastFillPrice) AS (
		SELECT 
			Symbol, Account, 
			LastFillQuantity * CASE SIDE WHEN 'S' THEN -1 ELSE 1 END  AS NetQuantity,
			LastFillQuantity * CASE SIDE WHEN 'S' THEN 0 ELSE 1 END  AS LongQuantity,
			LastFillQuantity * CASE SIDE WHEN 'S' THEN 1 ELSE 0 END  AS ShortQuantity,
			LastFillPrice
		FROM Trading.dbo.fills WITH(NOLOCK)
		WHERE date < CONVERT(DATETIME, '{0}') AND Symbol IN (SELECT [Contract] from Symbols)  AND Account = 'UNMBF222'
		--WHERE InsertTime < CONVERT(DATETIME, '{0}') AND Symbol IN (SELECT [Contract] from Symbols)  AND Account = 'UNMBF222'
		UNION ALL
		SELECT 
			Symbol, Account, 
			LastFillQuantity * CASE SIDE WHEN 'S' THEN -1 ELSE 1 END  AS NetQuantity,
			LastFillQuantity * CASE SIDE WHEN 'S' THEN 0 ELSE 1 END  AS LongQuantity,
			LastFillQuantity * CASE SIDE WHEN 'S' THEN 1 ELSE 0 END  AS ShortQuantity,
			LastFillPrice
		FROM Trading.dbo.backfill WITH(NOLOCK)
		WHERE date < CONVERT(DATETIME, '{0}') AND Symbol IN (SELECT [Contract] from Symbols) AND Account = 'UNMBF222' )

SELECT 
	 p.Symbol AS instrument,
	 'ED&F' AS exchange, 
	 Account AS account,
	 Position AS position,
	 Position * m.SettlementPrice * CASE WHEN p.Symbol LIKE 'ETH%' THEN 50 WHEN p.Symbol LIKE 'BTC%' THEN 5 ELSE 1 END AS notional,
	 m.SettlementPrice AS price,
	 (Position * m.SettlementPrice - EntryCost) * CASE WHEN p.Symbol LIKE 'ETH%' THEN 50 WHEN p.Symbol LIKE 'BTC%' THEN 5 ELSE 1 END AS unrealized_pnl,
	 'CME FUTURE' AS instrument_type,
	 s.Expiration  AS expiration_time, 
	 1  AS is_linear,
	 LEFT(p.Symbol,3) AS underlying,
	 EntryCost * CASE WHEN p.Symbol LIKE 'ETH%' THEN 50 WHEN p.Symbol LIKE 'BTC%' THEN 5 ELSE 1 END AS EntryCost
FROM 
	(SELECT 
		Symbol, 
        Account, 
		SUM(NetQuantity) AS Position, 
		SUM(LongQuantity) AS LongQuantity, 
		SUM(ShortQuantity) AS ShortQuantity,
		SUM(NetQuantity * LastFillPrice) AS EntryCost 
	FROM Positions 
	GROUP BY Symbol, Account) p
	JOIN Marks m ON p.Symbol = m.Symbol
	JOIN Symbols s ON p.Symbol = s.Contract;
"""

class TradingRepository:

    def __init__(self):
        pass

    def get_cme_positions(self, at_dtt_utc: dt.datetime):
        # maybe work out from trade list when we actually started tradeing this instrument
        return SqlQuery(GET_CME_POSITIONS,
                        at_dtt_utc.astimezone(ChicagoTimeZone).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                        at_dtt_utc.date().strftime('%Y-%m-%d'))
