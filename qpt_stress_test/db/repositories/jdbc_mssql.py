# -*- coding: utf-8 -*-
"""
"""
import datetime as dt

from .drivers.interfaces import SqlQueryInterface
# self._sql_query_class = SqlQuery
# self._db_connector_factory = qpt_sqlsvr_connectionfrom .drivers.jdbc 
# import SqlQuery
# from .drivers.jdbc import SqlQuery
# from ..tasks import jdbc_sqlsvr_connection
from qpt_stress_test.core.config import ChicagoTimeZone


GET_CME_POSITIONS = """
    SELECT 
        CONVERT(DATE, m.TradeDate) AS eod_date,
        p.as_of,
        'ED&F' AS exchange, 
        account,
        'CME FUTURE' AS instrument_type,
        p.Symbol AS instrument,
        CASE WHEN RIGHT([Contract], 2) = 'Z2' THEN CONVERT(DATETIME, '2022-12-30 00:00:00') 
            ELSE Expiration END AS expiration_time,
        1  AS is_linear,
        LEFT(p.Symbol,3) AS underlying,
        m.SettlementPrice AS price,
        Position AS position,
        Position * m.SettlementPrice * contract_size AS notional,
        (Position * m.SettlementPrice - EntryCost) * contract_size AS unrealized_pnl,
        EntryCost * contract_size AS EntryCost
    FROM 
        (
            SELECT 
                max([date]) AS as_of, Account,  Symbol, 
                CASE WHEN Symbol LIKE 'ETH%' THEN 50 WHEN Symbol LIKE 'BTC%' THEN 5 ELSE 1 END  AS contract_size, 
                SUM(Quantity) AS Position, 
                SUM(Quantity * LastFillPrice) AS EntryCost 
            FROM (
                SELECT [date], Symbol, Account, LastFillQuantity * CASE SIDE WHEN 'S' THEN -1 ELSE 1 END  AS Quantity, LastFillPrice
                FROM Trading.dbo.fills WITH(NOLOCK)
                UNION ALL
                SELECT [date], Symbol, Account, LastFillQuantity * CASE SIDE WHEN 'S' THEN -1 ELSE 1 END  AS Quantity, LastFillPrice
                FROM Trading.dbo.backfill WITH(NOLOCK)
            )  positions
            WHERE [date] <= '{as_of:%Y-%m-%d %H:%M:%S}'
            GROUP BY Account, Symbol
        ) p
        JOIN trading.crypto.CMEOfficialSettlement m WITH(NOLOCK) ON p.Symbol = m.Symbol
        JOIN Trading.dbo.products s WITH(NOLOCK) ON p.Symbol = s.Contract
    WHERE p.Position <> 0  AND p.Account in {accounts} and convert(DATE, m.TradeDate) = '{as_of:%Y-%m-%d}'
"""

GET_LAST_TRADING_BALANCES_EOD_TRADEDATE = "SELECT Max(TradeDate) as last_date FROM trading.v2.EndOfDayBalance"

GET_OPERATIONS_EOD_BALANCES = """
    select
        a.Account, a.Balance, a.BalanceType, UPPER(a.Currency) as Currency, Isnull(b.TableName, '') as Source, 
        FORMAT(a.Date_UTC,'yyyyMMdd') as Timestamp, a.AsOf_UTC as Timestamp_Native
    from
        Operations.balances.EndOfDay_00UTC A 
            LEFT JOIN Operations.balances.sources B on a.Account = b.Account
    where 
        balance != 0 
        and a.Date_UTC = '{eod_date:%Y%m%d}' 
"""

"""
DECLARE @T_DATE AS DATE; SET @T_DATE = '20221129';

	SELECT DISTINCT SecurityExchange as ex_endpoint, BFCSymbol as symbol_bfc, ContractMultiplier AS qty_multiplier, NULL AS lot_Size, Symbol as symbol_exch, NULL AS currency_position, 
	          NULL AS currency_quote, NULL AS IsInverse, SecurityType AS Inst_Type 
	FROM [Securities].dbo.CryptoSecurities WITH(NOLOCK)
	WHERE is_active = 1 AND BFCSymbol in (SELECT distinct symbol FROM trading.crypto.EndOfDay WITH(NOLOCK) WHERE CONVERT(DATE,TradeDate) = @T_DATE) 
	ORDER BY  ex_endpoint, symbol_bfc;

	SELECT DISTINCT 
		CASE WHEN exchange = 'BINANCE' THEN 'BINE' WHEN EXCHANGE = 'Huobi' OR exchange = 'HUOBI' THEN 'HUBI' WHEN exchange = 'Kraken' THEN 'KRKN' ELSE exchange END as ex_endpoint, 
			bfcSymbol as symbol_bfc, Multiplier AS qty_multiplier, lotSize AS lot_Size, exchSymbol as symbol_exch, positionCurrency AS currency_position, rootsymbol ,
	           Quotecurrency AS currency_quote, IsInverse, Expiry, class AS Inst_Type
	FROM [Securities].dbo.CryptoFutures WITH(NOLOCK)
	WHERE is_active = 1 AND Expiry >  @T_DATE AND BFCSymbol in (SELECT distinct symbol FROM trading.crypto.EndOfDay WITH(NOLOCK) WHERE CONVERT(DATE,TradeDate) = @T_DATE) 
	ORDER BY ex_endpoint, symbol_bfc;
"""

# Client Data
"""
SELECT 
    CONVERT(DATE,YYYYMMDD) AS tradedate, 
    Client, 
    Account, 
    endpoint, 
    eod.Symbol, 
    CASE WHEN crypto_inst.Inst_Type IS NOT NULL THEN crypto_inst.Inst_Type WHEN cme_inst.SecurityType IS NOT NULL THEN cme_inst.SecurityType ELSE '' END AS instrument_type, 
    CASE WHEN crypto_inst.Expiry IS NOT NULL THEN crypto_inst.Expiry  WHEN  cme_inst.Expiration IS NOT NULL THEN cme_inst.Expiration ELSE NULL END AS expiry, 
    CASE WHEN crypto_inst.IsInverse IS NOT NULL THEN crypto_inst.IsInverse  ELSE 0 END AS is_inverse, 
    CASE WHEN cme_inst.currency_position IS NOT NULL THEN cme_inst.currency_position
         WHEN crypto_inst.currency_position IS NOT NULL AND crypto_inst.currency_position <> '' THEN crypto_inst.currency_position 
         WHEN crypto_inst.rootsymbol IS NOT NULL THEN crypto_inst.rootsymbol 
         ELSE MAX(CASE WHEN [TYPE] = 'BALANCE' THEN product ELSE '' END) END AS inst_position_currency,
    marks.closingprice,
    txn_count,	
    today_short_quantity,
    today_long_quantity, 
    sum(CASE WHEN [TYPE] = 'BALANCE' THEN value ELSE 0 END) AS eodposition,
    MAX(CASE WHEN [TYPE] = 'BALANCE' THEN product ELSE '' END ) AS eodposition_currency,
    sum(CASE WHEN [TYPE] = 'PNL' THEN value ELSE 0 END) AS pnl,
    MAX(CASE WHEN [TYPE] = 'PNL' THEN product ELSE '' END ) AS pnl_currency,
    sum(CASE WHEN [TYPE] = 'FEE' THEN value ELSE 0 END) AS fee,
    sum(CASE WHEN [TYPE] = 'DELTA_OFFSET' THEN value ELSE 0 END) AS delta_offset,
    sum(CASE WHEN [TYPE] = 'FUNDING' THEN value ELSE 0 END) AS funding,
    sum(CASE WHEN [TYPE] = 'QUANTITYxPRICE' THEN value ELSE 0 END) AS quantity_by_price,
    sum(CASE WHEN [TYPE] = 'QUANTITY/PRICE' THEN value ELSE 0 END) AS quantity_over_price,
    isnull(marks.closingprice, 1) * sum(CASE WHEN [TYPE] = 'BALANCE' THEN value ELSE 0 END) AS notional 
FROM trading.pnl.EOD_NEW eod WITH(NOLOCK)
  LEFT JOIN  (
    SELECT DISTINCT 
        CASE WHEN exchange = 'BINANCE' THEN 'BINE' 
             WHEN EXCHANGE = 'Huobi' OR exchange = 'HUOBI' THEN 'HUBI' 
             WHEN exchange = 'Kraken' THEN 'KRKN' 
             ELSE exchange END as ex_endpoint, 
        bfcSymbol as symbol_bfc, 
        Multiplier AS qty_multiplier, 
        lotSize AS lot_Size, 
        exchSymbol as symbol_exch, 
        rootsymbol,
        positionCurrency AS currency_position,
        Quotecurrency AS currency_quote,
        IsInverse, 
        class AS Inst_Type, 
        Expiry
    FROM [Securities].dbo.CryptoFutures WITH(NOLOCK)
    WHERE is_active = 1 ) crypto_inst ON eod.Symbol = crypto_inst.symbol_bfc AND eod.endpoint = ex_endpoint
  LEFT JOIN (
    SELECT YYYYMMDD as txn_YYYYMMDD, client as txn_client, ACCOUNT as txn_ACCOUNT, endpoint as txn_endpoint, symbol as txn_symbol, 
        count(distinct tid) AS txn_count, 
        SUM(LastFillQuantity * CASE SIDE WHEN 'S' THEN -1 ELSE 0 END) / 1000000000.0 as today_short_quantity,
        SUM(LastFillQuantity * CASE SIDE WHEN 'S' THEN 0 ELSE 1 END) / 1000000000.0 as today_long_quantity
    FROM trading.crypto.backfill tx WITH(NOLOCK)
    WHERE YYYYMMDD = '{0:%Y%m%d}'
    GROUP BY YYYYMMDD, client, ACCOUNT, endpoint, symbol) txns ON txn_YYYYMMDD = eod.YYYYMMDD AND txn_client = client AND  txn_ACCOUNT = account and txn_endpoint = endpoint AND txn_symbol = eod.symbol
  LEFT JOIN  (
    SELECT [Contract] AS symbol_bfc, Exchange, Multiplier, SecurityType, Expiration, Product AS currency_position, CurrencyCode AS currency_quote
    FROM Trading.dbo.products WITH(NOLOCK)
    WHERE Exchange = 'CME' AND SecurityType = 'FUTURE') cme_inst ON eod.Symbol = cme_inst.symbol_bfc
  LEFT JOIN (
    SELECT Symbol As mark_symbol, TradeDate as mark_date, avg(ClosingPrice) as closingprice
    FROM trading.crypto.marksettle WITH(NOLOCK)
    GROUP BY Symbol, TradeDate) marks ON marks.mark_symbol = eod.symbol AND marks.mark_date = YYYYMMDD
 WHERE YYYYMMDD = '{0:%Y%m%d}'  AND (crypto_inst.Expiry > '{0:%Y%m%d}' OR cme_inst.Expiration > '{0:%Y%m%d}' OR (crypto_inst.Expiry is null and cme_inst.Expiration is null))
 GROUP BY YYYYMMDD, endpoint, Client, Account, Symbol, crypto_inst.Expiry, cme_inst.Expiration, crypto_inst.rootsymbol, cme_inst.currency_position, crypto_inst.IsInverse, 
    crypto_inst.currency_position, crypto_inst.Inst_Type, cme_inst.SecurityType, marks.closingprice, txn_count,	today_short_quantity, today_long_quantity
 HAVING 
    NOT(abs(sum(CASE WHEN [TYPE] = 'BALANCE' THEN value ELSE 0 END)) <= 1e-08)
 ORDER BY YYYYMMDD, Client, endpoint, Account, Symbol;
"""


class TradingRepository:


    def __init__(self, sql_query_driver, db_connector_factory):
        self._sql_query_class = SqlQuery
        self._db_connector_factory = qpt_sqlsvr_connection
    
    def adhoc_query(self, sql: str) -> SqlQueryInterface:
        return self._sql_query_class(
            sql, 
            pyodbc_conn_fn=self._db_connector_factory)

    @property
    def last_positions_date(self) -> dt.date:
        _, data = self._sql_query_class(
            GET_LAST_TRADING_BALANCES_EOD_TRADEDATE, 
            db_connector_factory=self._db_connector_factory).as_list()
        return data[0][0].date()

    def get_cme_positions(self, at_dtt_utc: dt.datetime) -> SqlQueryInterface:
        return self._sql_query_class(
            GET_CME_POSITIONS.format(accounts=('UNMBF222', ''), as_of=at_dtt_utc.astimezone(ChicagoTimeZone)),
            db_connector_factory=self._db_connector_factory)

    def get_operations_eod_balances(self, eod_date: dt.date) -> SqlQueryInterface:
        return self._sql_query_class(
            GET_OPERATIONS_EOD_BALANCES.format(eod_date=eod_date),
            db_connector_factory=self._db_connector_factory)

    def get_trading_eod_balances(self, eod_date: dt.date) -> SqlQueryInterface:
        # Todo: replace with call against trading.EOD_new or similar
        return self._sql_query_class(
            GET_OPERATIONS_EOD_BALANCES.format(trade_date=eod_date),
            db_connector_factory=self._db_connector_factory)


GET_COINMARKETCAP_CLOSE_MARKS = """
    select 
        format(timestamp,'yyyyMMdd') as DATE, symbol as Currency, _close as Mark, 'coinmarketcap' as Source
    from 
        RawData.cnmk_1_m.ohlcv_historical
    where 
        format(timestamp,'yyyyMMdd') = '{close_date:%Y%m%d}' and
        name not in ('CyberMiles','Genesis Mana','NFT','NEFTiPEDiA','UNICORN Token','DEONEX COIN','Don-key','LOLTOKEN','SoMee.Social [OLD]')
    order by  DATE, symbol;
"""

GET_TRADING_CLOSE_MARKS = """
    select DATE, Currency, Mark, Source 
    from
        trading.pnl.marks
    where DATE = '{close_date:%Y%m%d}'
    order by  DATE, Currency;
"""


class MarketDataRepository:

    def __init__(self, sql_query_driver, db_connector_factory):
        self._sql_query_class = sql_query_driver
        self._db_connector_factory = db_connector_factory 

    def get_coinmarketcap_close_marks(self, close_date: dt.date) -> SqlQueryInterface:
        return self._sql_query_class(
            GET_COINMARKETCAP_CLOSE_MARKS.format(close_date=close_date),
            db_connector_factory=self._db_connector_factory)

    def get_trading_close_marks(self, close_date: dt.date) -> SqlQueryInterface:
        return self._sql_query_class(
            GET_TRADING_CLOSE_MARKS.format(close_date=close_date),
            db_connector_factory=self._db_connector_factory)
