# -*- coding: utf-8 -*-
"""
"""
import datetime as dt

from .drivers.interfaces import SqlQueryInterface
from qpt_stress_test.core.config import ChicagoTimeZone

GET_OPERATIONS_BALANCES_LAST_DATE_UTC = "SELECT Max(Date_UTC) as last_date FROM Operations.balances.EndOfDay_00UTC;"

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


class OperationsRepository:

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
            GET_OPERATIONS_BALANCES_LAST_DATE_UTC,
            db_connector_factory=self._db_connector_factory).as_list()
        return data[0][0].date()

    def get_eod_balances(self, eod_date: dt.date) -> SqlQueryInterface:
        return self._sql_query_class(
            GET_OPERATIONS_EOD_BALANCES.format(eod_date=eod_date),
            db_connector_factory=self._db_connector_factory)


GET_TRADING_BALANCES_LAST_TRADEDATE = "SELECT Max(TradeDate) as last_date FROM trading.v2.EndOfDayBalance;"

# crypto.bankbalances: ['ID', 'api_name', 'bfc_name', 'currency', 'balance', 'trade_date', 'InsertTime', 'source']
GET_CRYPTO_BANKBALANCES = """
    select bfc_name as Account, balance as Balance, 'Cash' as BalanceType, UPPER(currency) as Currency,
        'crypto.bankbalances' as Source, trade_date as Timestamp, InsertTime as Timestamp_Native
    from trading.crypto.bankbalances 
    where trade_date = '{eod_date:%Y%m%d}'
"""

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

# trading.pnl.EOD_NEW positions
# Trading balances by client - swaps and futures look to match out against actual positions, if you remove symbols that we don't have positions in.
# But there seems to be no easy way of working out which coin balances are current or stale
# Most likely, we would need to get valide Client/Account/Symbol values from some other source to be usable by risk
"""
	SELECT CONVERT(DATE,YYYYMMDD) AS tradedate, Client, endpoint, Account, eod.Symbol, 
		MAX(CASE WHEN [TYPE] = 'BALANCE' THEN product ELSE '' END ) AS eodposition_currency,
		MAX(CASE WHEN [TYPE] = 'PNL' THEN product ELSE '' END ) AS pnl_currency,
		sum(CASE WHEN [TYPE] = 'BALANCE' THEN value ELSE 0 END) AS eodposition,
		sum(CASE WHEN [TYPE] = 'PNL' THEN value ELSE 0 END) AS pnl,
		sum(CASE WHEN [TYPE] = 'FEE' THEN value ELSE 0 END) AS fee,
		sum(CASE WHEN [TYPE] = 'DELTA_OFFSET' THEN value ELSE 0 END) AS delta_offset,
		sum(CASE WHEN [TYPE] = 'FUNDING' THEN value ELSE 0 END) AS funding,
		sum(CASE WHEN [TYPE] = 'QUANTITYxPRICE' THEN value ELSE 0 END) AS quantity_by_price,
		sum(CASE WHEN [TYPE] = 'QUANTITY/PRICE' THEN value ELSE 0 END) AS quantity_over_price
	FROM trading.pnl.EOD_NEW eod WITH(NOLOCK)
	WHERE YYYYMMDD = '{eod_date:%Y%m%d}' 
		AND (Account in (SELECT Account FROM Operations.balances.EndOfDay_00UTC WHERE balance <> 0  and Date_UTC = @T_DATE ) 
		      OR Endpoint in ('FEE', 'FUND', 'LEND', 'REBATE', 'REWARD', 'YIELD', 'CME'))--  AND Symbol in ('BLINKUSDT', 'O_P_LINK-USDT-SWAP', 'LINK', 'LINKUSD', 'LINKUSDT') AND Account in ('BINE-2-S1-E')
	GROUP BY YYYYMMDD, endpoint, Client, Account, Symbol
	HAVING NOT(abs(sum(CASE WHEN [TYPE] = 'BALANCE' THEN value ELSE 0 END)) <= 1e-08) 
	ORDER BY Client, account, symbol
"""
"""
	SELECT CONVERT(DATE,YYYYMMDD) AS tradedate, Client, endpoint, Account, eod.Symbol, 
		txn_count,	
		CASE WHEN crypto_inst.Inst_Type IS NOT NULL THEN crypto_inst.Inst_Type WHEN cme_inst.SecurityType IS NOT NULL THEN cme_inst.SecurityType ELSE '' END AS instrument_type, 
		CASE WHEN crypto_inst.Expiry IS NOT NULL THEN crypto_inst.Expiry  WHEN  cme_inst.Expiration IS NOT NULL THEN cme_inst.Expiration ELSE NULL END AS expiry, 
		CASE WHEN crypto_inst.IsInverse IS NOT NULL THEN crypto_inst.IsInverse  ELSE 0 END AS is_inverse, 
		CASE WHEN cme_inst.currency_position IS NOT NULL THEN cme_inst.currency_position
		     WHEN crypto_inst.currency_position IS NOT NULL AND crypto_inst.currency_position <> '' THEN crypto_inst.currency_position 
		     WHEN crypto_inst.rootsymbol IS NOT NULL THEN crypto_inst.rootsymbol 
			 ELSE MAX(CASE WHEN [TYPE] = 'BALANCE' THEN product ELSE '' END) END AS inst_position_currency,
		marks.closingprice,
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
			CASE WHEN exchange = 'BINANCE' THEN 'BINE' WHEN EXCHANGE = 'Huobi' OR exchange = 'HUOBI' THEN 'HUBI' WHEN exchange = 'Kraken' THEN 'KRKN' ELSE exchange END as ex_endpoint, 
			bfcSymbol as symbol_bfc, Multiplier AS qty_multiplier, lotSize AS lot_Size, exchSymbol as symbol_exch, rootsymbol,
			positionCurrency AS currency_position,
			Quotecurrency AS currency_quote,
			IsInverse, class AS Inst_Type , Expiry
		FROM [Securities].dbo.CryptoFutures WITH(NOLOCK)
		WHERE is_active = 1 ) crypto_inst ON eod.Symbol = crypto_inst.symbol_bfc AND eod.endpoint = ex_endpoint
	  LEFT JOIN (
		SELECT YYYYMMDD as txn_YYYYMMDD, client as txn_client, ACCOUNT as txn_ACCOUNT, endpoint as txn_endpoint, symbol as txn_symbol, 
			count(distinct tid) AS txn_count, 
			SUM(LastFillQuantity * CASE SIDE WHEN 'S' THEN -1 ELSE 0 END) / 1000000000.0 as today_short_quantity,
			SUM(LastFillQuantity * CASE SIDE WHEN 'S' THEN 0 ELSE 1 END) / 1000000000.0 as today_long_quantity
		FROM trading.crypto.backfill tx WITH(NOLOCK)
		WHERE YYYYMMDD = @T_DATE
		GROUP BY YYYYMMDD, client, ACCOUNT, endpoint, symbol
	  ) txns ON txn_YYYYMMDD = eod.YYYYMMDD AND txn_client = client AND  txn_ACCOUNT = account and txn_endpoint = endpoint AND txn_symbol = eod.symbol
	  LEFT JOIN  (
		SELECT [Contract] AS symbol_bfc, Exchange, Multiplier, SecurityType, Expiration, Product AS currency_position, CurrencyCode AS currency_quote
		FROM Trading.dbo.products WITH(NOLOCK)
		WHERE Exchange = 'CME' AND SecurityType = 'FUTURE') cme_inst ON eod.Symbol = cme_inst.symbol_bfc
	  LEFT JOIN (
		SELECT Symbol As mark_symbol, TradeDate as mark_date, avg(ClosingPrice) as closingprice
		FROM trading.crypto.marksettle WITH(NOLOCK)
		GROUP BY Symbol, TradeDate) marks ON marks.mark_symbol = eod.symbol AND marks.mark_date = YYYYMMDD
	 WHERE YYYYMMDD = @T_DATE  AND (crypto_inst.Expiry > @T_DATE OR cme_inst.Expiration > @T_DATE OR (crypto_inst.Expiry is null and cme_inst.Expiration is null))
	 GROUP BY YYYYMMDD, endpoint, Client, Account, Symbol, crypto_inst.Expiry, cme_inst.Expiration, crypto_inst.rootsymbol, cme_inst.currency_position, crypto_inst.IsInverse, 
		crypto_inst.currency_position, crypto_inst.Inst_Type, cme_inst.SecurityType, marks.closingprice, txn_count,	today_short_quantity, today_long_quantity
	 HAVING 
		NOT(abs(sum(CASE WHEN [TYPE] = 'BALANCE' THEN value ELSE 0 END)) <= 1e-08)
	 ORDER BY YYYYMMDD, Client, endpoint, Account, Symbol
"""


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
            GET_TRADING_BALANCES_LAST_TRADEDATE,
            db_connector_factory=self._db_connector_factory).as_list()
        return data[0][0].date()

    def get_bank_balances(self, eod_date: dt.date) -> SqlQueryInterface:
        return self._sql_query_class(
            GET_CRYPTO_BANKBALANCES.format(eod_date=eod_date),
            db_connector_factory=self._db_connector_factory)

    def get_cme_positions(self, at_dtt_utc: dt.datetime) -> SqlQueryInterface:
        return self._sql_query_class(
            GET_CME_POSITIONS.format(accounts=('UNMBF222', ''), as_of=at_dtt_utc.astimezone(ChicagoTimeZone)),
            db_connector_factory=self._db_connector_factory)

    def get_operations_eod_balances(self, eod_date: dt.date) -> SqlQueryInterface:
        # Todo: remove this & use OperationsRepository.get_eod_balances
        return self._sql_query_class(
            GET_OPERATIONS_EOD_BALANCES.format(eod_date=eod_date),
            db_connector_factory=self._db_connector_factory)

    def get_trading_eod_balances(self, eod_date: dt.date) -> SqlQueryInterface:
         # Todo: replace with call against trading.EOD_new or similar
        return self._sql_query_class(
            GET_OPERATIONS_EOD_BALANCES.format(eod_date=eod_date),
            db_connector_factory=self._db_connector_factory)


GET_RAWDATA_EXCHANGE_BALANCES = """
SELECT 
    Account, balance as Balance, BalanceType, UPPER(currency) as Currency,
    CONCAT('RawData.', endpoint) as Source, CONVERT(DATE, asof) as Timestamp, asof as Timestamp_Native
FROM (
/**/  SELECT asset as currency, 'BINE' as endpoint, 'BINE-1-M-M' AS Account, '' AS BalanceType, netAsset as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY Timestamp DESC) as rn 
  from RawData.bine_1_m_m.margin_account_asset WITH(NOLOCK) WHERE timestamp BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}' 
  UNION
/**/  SELECT asset as currency, 'BINE' as endpoint, 'BINE-2-S1-E' AS Account, '' AS BalanceType, (free+locked) as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY Timestamp DESC) as rn 
  from RawData.bine_2_s1_e.account_asset WITH(NOLOCK) WHERE timestamp BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}' 
  UNION
  SELECT asset as currency, 'BINE' as endpoint, 'BINE-2-S1-F' AS Account, '' AS BalanceType, WalletBalance as balance, asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY asof DESC) as rn 
  from RawData.bine_2_s1_f.account_asset WITH(NOLOCK) WHERE AsOf BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT asset as currency, 'BINE' as endpoint, 'BINE-2-S1-F' AS Account, 'Unrealized' AS BalanceType, unrealizedProfit as balance, asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY asof DESC) as rn  
  from RawData.bine_2_s1_f.account_asset WITH(NOLOCK) WHERE AsOf BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT asset as currency, 'BINE' as endpoint, 'BINE-2-S1-M' AS Account, '' AS BalanceType, netAsset as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY Timestamp DESC) as rn 
  from RawData.bine_2_s1_m.margin_account_asset WITH(NOLOCK) WHERE timestamp BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT asset as currency, 'BINE' as endpoint, 'BINE-2-S1-P' AS Account, '' AS BalanceType, WalletBalance as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY Timestamp DESC) as rn 
  from RawData.bine_2_s1_p.futures_account_assets WITH(NOLOCK) WHERE timestamp BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT asset as currency, 'BINE' as endpoint, 'BINE-2-S1-P' AS Account, 'Unrealized' AS BalanceType, unrealizedProfit as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY Timestamp DESC) as rn 
  from RawData.bine_2_s1_p.futures_account_assets WITH(NOLOCK) WHERE timestamp BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
/**/  SELECT asset as currency, 'BINE' as endpoint, 'BINE-2-S2-E' AS Account, '' AS BalanceType, (free+locked) as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY Timestamp DESC) as rn 
  from RawData.bine_2_s2_e.account_asset WITH(NOLOCK) WHERE timestamp BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
/**/  SELECT asset as currency, 'BINE' as endpoint, 'BINE-2-S2-F' AS Account, '' AS BalanceType, WalletBalance as balance, asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY asof DESC) as rn 
  from RawData.bine_2_s2_f.account_asset WITH(NOLOCK) WHERE AsOf BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
/**/  SELECT asset as currency, 'BINE' as endpoint, 'BINE-2-S2-F' AS Account, 'Unrealized' AS BalanceType, unrealizedProfit as balance, asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY asof DESC) as rn 
  from RawData.bine_2_s2_f.account_asset WITH(NOLOCK) WHERE AsOf BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
/**/  SELECT asset as currency, 'BINE' as endpoint, 'BINE-2-S2-M' AS Account, '' AS BalanceType, netAsset as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY Timestamp DESC) as rn 
  from RawData.bine_2_s2_m.margin_account_asset WITH(NOLOCK) WHERE timestamp BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT asset as currency, 'BINE' as endpoint, 'BINE-2-S2-P' AS Account, '' AS BalanceType, WalletBalance as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY Timestamp DESC) as rn 
  from RawData.bine_2_s2_p.futures_account_assets WITH(NOLOCK) WHERE timestamp BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT asset as currency, 'BINE' as endpoint, 'BINE-2-S2-P' AS Account, 'Unrealized' AS BalanceType, unrealizedProfit as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY Timestamp DESC) as rn  
  from RawData.bine_2_s2_p.futures_account_assets WITH(NOLOCK) WHERE timestamp BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
/**/  SELECT asset as currency, 'BINE' as endpoint, 'BINE-MX-S1-E' AS Account, '' AS BalanceType, (free+locked) as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY Timestamp DESC) as rn 
  from RawData.bine_mx_s1_e.account_asset WITH(NOLOCK) WHERE timestamp BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
/**/  SELECT asset as currency, 'BINE' as endpoint, 'BINE-MX-S1' AS Account, '' AS BalanceType, netAsset as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY Timestamp DESC) as rn 
  from RawData.bine_mx_s1_m.margin_account_asset WITH(NOLOCK) WHERE timestamp BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
/**/  SELECT asset as currency, 'BINE' as endpoint, 'BINE-MX-S1-F' AS Account, '' AS BalanceType, WalletBalance as balance, asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY asof DESC) as rn 
  from RawData.bine_mx_s1_f.account_asset WITH(NOLOCK) WHERE AsOf BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
/**/  SELECT asset as currency, 'BINE' as endpoint, 'BINE-MX-S1-P' AS Account, '' AS BalanceType, WalletBalance as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY Timestamp DESC) as rn 
  from RawData.bine_mx_s1_p.futures_account_assets WITH(NOLOCK) WHERE timestamp BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
/**/  SELECT asset as currency, 'BINE' as endpoint, 'BINE-MX-S1-P' AS Account, 'Unrealized' AS BalanceType, unrealizedProfit as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY Timestamp DESC) as rn 
  from RawData.bine_mx_s1_p.futures_account_assets WITH(NOLOCK) WHERE timestamp BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT currency as currency, 'BTFX' as endpoint, 'BTFX-1-M-E' AS Account, '' AS BalanceType, balance as balance, asof_CST as asof, ROW_NUMBER() OVER (PARTITION BY wallet_type, currency ORDER BY asof_CST DESC) as rn 
  from RawData.btfx_1_m_e.wallet_balance WITH(NOLOCK) WHERE wallet_type = 'exchange' AND asof_CST BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}' 
  UNION
  SELECT currency as currency, 'BTFX' as endpoint, 'BTFX-1-M-E' AS Account, wallet_type AS BalanceType, balance as balance, asof_CST as asof, ROW_NUMBER() OVER (PARTITION BY wallet_type, currency ORDER BY asof_CST DESC) as rn 
  from RawData.btfx_1_m_e.wallet_balance WITH(NOLOCK) WHERE wallet_type <> 'exchange' AND asof_CST BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT symbol as currency, 'BULL' as endpoint, 'BULL-1-M-E' AS Account, '' AS BalanceType, Total as balance, asof_CST as asof, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY asof_CST DESC) as rn  
  from RawData.bull_1_m.spot_account WITH(NOLOCK) WHERE asof_CST BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT symbol as currencye, 'BULL' as endpoint, 'BULL-2-M-E' AS Account, '' AS BalanceType, Total as balanc, asof_CST as asof, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY asof_CST DESC) as rn 
  from RawData.bull_2_m.spot_account WITH(NOLOCK) WHERE asof_CST BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT symbol as currency, 'BULL' as endpoint, 'BULL-3-M-E' AS Account, '' AS BalanceType, Total as balance, asof_CST as asof, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY asof_CST DESC) as rn 
  from RawData.bull_3_m.spot_account WITH(NOLOCK) WHERE asof_CST BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT symbol as currency, 'BULL' as endpoint, 'BULL-4-M-E' AS Account, '' AS BalanceType, Total as balance, asof_CST as asof, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY asof_CST DESC) as rn 
  from RawData.bull_4_m.spot_account WITH(NOLOCK) WHERE asof_CST BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT currency as currency, bfc_name as endpoint, bfc_name AS Account, '' AS BalanceType, balance as balance, InsertTime as asof, ROW_NUMBER() OVER (PARTITION BY bfc_name, currency ORDER BY InsertTime DESC) as rn 
  from Trading.crypto.BankBalances WITH(NOLOCK) WHERE InsertTime BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT Currency as currency, Exchange as endpoint, Exchange AS Account, '' AS BalanceType, Value as balance, InsertTime as asof, ROW_NUMBER() OVER (PARTITION BY Exchange, Currency ORDER BY InsertTime DESC) as rn 
  from Trading.crypto.SnapshotBalances WITH(NOLOCK) WHERE Exchange in ('BTSE', 'KRKE', 'LMAC', 'LMAX') AND InsertTime BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
  (SELECT TOP 1 'usd' as currency, 'DYDX' as endpoint, 'DYDX-1-M' AS Account, '' AS BalanceType, equity as balance, asof, 1 as rn  
  from RawData.dydx_1_m.account WITH(NOLOCK) ORDER BY AsOf DESC)
  UNION
  SELECT currency as currency, /*type,*/ 'HUBI' as endpoint, 'HUBI-3-S1 AB' AS Account, BalanceType, Balance as balance, asof, /*ROW_NUMBER() OVER (PARTITION BY currency, type, BalanceType ORDER BY AsOf DESC)*/ 1 as rn 
  from RawData.hubi_3_s1.account_balance WITH(NOLOCK) WHERE (SELECT MAX(asof) from RawData.hubi_3_s1.account_balance WITH(NOLOCK)) >= '{from_ts:%Y-%m-%d %H:%M:%S}' AND asof = (SELECT MAX(asof) from RawData.hubi_3_s1.account_balance WITH(NOLOCK) WHERE AsOf BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}' )
  UNION
  SELECT currency as currency, /*type,*/ 'HUBI' as endpoint, 'HUBI-3-S2 AB' AS Account, BalanceType, Balance as balance, asof,  /*ROW_NUMBER() OVER (PARTITION BY currency, type, BalanceType ORDER BY AsOf DESC)*/1 as rn 
  from RawData.hubi_3_s2.account_balance WITH(NOLOCK) WHERE (SELECT MAX(asof) from RawData.hubi_3_s2.account_balance WITH(NOLOCK)) >= '{from_ts:%Y-%m-%d %H:%M:%S}' AND asof = (SELECT MAX(asof) from RawData.hubi_3_s2.account_balance WITH(NOLOCK) WHERE AsOf BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}' )
  UNION
  SELECT currency as currency, /*type,*/ 'HUBI' as endpoint, 'HUBI-3-S3-M' AS Account, BalanceType, Balance as balance, asof, /*ROW_NUMBER() OVER (PARTITION BY currency, type, BalanceType ORDER BY AsOf DESC)*/ 1 as rn 
  from RawData.hubi_3_s3.account_balance WITH(NOLOCK) WHERE (SELECT MAX(asof) from RawData.hubi_3_s3.account_balance WITH(NOLOCK)) >= '{from_ts:%Y-%m-%d %H:%M:%S}' AND asof = (SELECT MAX(asof) from RawData.hubi_3_s3.account_balance WITH(NOLOCK) WHERE AsOf BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}' )
  UNION 
  SELECT currency as currency, /*type,*/ 'HUBI' as endpoint, 'HUB2-M' AS Account, BalanceType, Balance as balance, asof, /*ROW_NUMBER() OVER (PARTITION BY currency, type, BalanceType ORDER BY AsOf DESC)*/ 1 as rn 
  from RawData.hub2.account_balance WITH(NOLOCK) WHERE (SELECT MAX(asof) from  RawData.hub2.account_balance WITH(NOLOCK)) >= '{from_ts:%Y-%m-%d %H:%M:%S}' AND asof = (SELECT MAX(asof) from  RawData.hub2.account_balance WITH(NOLOCK) WHERE AsOf BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}' )
  UNION
  SELECT currency as currency,  /*type,*/ 'HUBI' as endpoint, 'HUBI-E' AS Account, '' AS BalanceType, sum(Balance) as balance, asof, ROW_NUMBER() OVER (PARTITION BY currency, type, BalanceType ORDER BY AsOf DESC) as rn 
  from RawData.hubi.account_balance WITH(NOLOCK) WHERE AsOf BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}' GROUP BY currency, type, balancetype, asof HAVING type = 'spot' AND balancetype = 'trade' 
  UNION
  SELECT currency as currency, 'HUBI' as endpoint, 'HUBI-M' AS Account, '' AS BalanceType, sum(Balance) as balance, asof, ROW_NUMBER() OVER (PARTITION BY currency, type, BalanceType ORDER BY AsOf DESC) as rn 
  from RawData.hubi.account_balance WITH(NOLOCK) WHERE AsOf BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}' GROUP BY currency, type, balancetype, asof HAVING type = 'margin' AND balancetype = 'trade' 
  UNION
  SELECT currency as currency, 'HUBI' as endpoint, 'HUBI-M' AS Account, 'MarginLoan' AS BalanceType, sum(Balance) as balance, asof, ROW_NUMBER() OVER (PARTITION BY currency, type, BalanceType ORDER BY AsOf DESC) as rn 
  from RawData.hubi.account_balance WITH(NOLOCK) WHERE AsOf BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}' GROUP BY currency, type, balancetype, asof HAVING type = 'margin' AND balancetype = 'loan' 
  UNION
  SELECT currency as currency, 'HUBI' as endpoint, 'HUBI-E' AS Account, '' AS BalanceType, Balance as balance, asof, ROW_NUMBER() OVER (PARTITION BY currency, type, BalanceType ORDER BY AsOf DESC) as rn 
  from RawData.hubi.account_balance WITH(NOLOCK) WHERE TYPE = 'spot' AND BalanceType = 'trade' AND AsOf BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}' 
  UNION
  SELECT symbol as currency, /*contract_code,*/ 'HUBI' as endpoint, 'HUBI-1-M-P' AS Account, '' AS BalanceType, margin_balance as balance, asof, 1 as rn
  from RawData.hubi_1_m_p.swap_account_info WITH(NOLOCK) WHERE (SELECT MAX(asof) from RawData.hubi_1_m_p.swap_account_info WITH(NOLOCK)) >= '{from_ts:%Y-%m-%d %H:%M:%S}' AND asof = (SELECT MAX(asof) from RawData.hubi_1_m_p.swap_account_info WITH(NOLOCK) WHERE AsOf BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}' )
  UNION
  SELECT symbol as currency, /*contract_code,*/ 'HUBI' as endpoint, 'HUBI-1-M-P' AS Account, 'Unrealized' AS BalanceType, (profit_unreal) as balance, asof,  1 as rn
  from RawData.hubi_1_m_p.swap_account_info WITH(NOLOCK) WHERE (SELECT MAX(asof) from RawData.hubi_1_m_p.swap_account_info WITH(NOLOCK)) >= '{from_ts:%Y-%m-%d %H:%M:%S}' AND asof = (SELECT MAX(asof) from RawData.hubi_1_m_p.swap_account_info WITH(NOLOCK) WHERE AsOf BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}' )
  UNION
  SELECT symbol as currency, 'HUBI' as endpoint, 'HUBI-3-S1' AS Account, '' AS BalanceType, (margin_balance) as balance, asof, ROW_NUMBER() OVER (PARTITION BY symbol, contract_code ORDER BY asof DESC) as rn 
  from RawData.hubi_3_s1.swap_account_info  WITH(NOLOCK) WHERE AsOf BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
/**/  SELECT currency as currency, 'HUBI' as endpoint, 'HUBI-3-S1' AS Account, 'margin_loan' AS BalanceType, Balance as balance, asof, ROW_NUMBER() OVER (PARTITION BY symbol, Currency, Type, CurrencyType ORDER BY asof DESC) as rn  
  from RawData.hubi_3_s1.isolated_margin_loan_balance WITH(NOLOCK) WHERE AsOf BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}' 
  UNION
  SELECT symbol as currency, 'HUBI' as endpoint, 'HUBI-3-S1' AS Account, 'Unrealized' AS BalanceType, (profit_unreal) as balance, asof, ROW_NUMBER() OVER (PARTITION BY symbol, contract_code ORDER BY asof DESC) as rn 
  from RawData.hubi_3_s1.swap_account_info  WITH(NOLOCK) WHERE AsOf BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT symbol as currency, 'HUBI' as endpoint, 'HUBI-3-S2' AS Account, '' AS BalanceType, (margin_balance) as balance, asof, ROW_NUMBER() OVER (PARTITION BY symbol, contract_code ORDER BY asof DESC) as rn 
  from RawData.hubi_3_s2.swap_account_info WITH(NOLOCK) WHERE  AsOf BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT currency as currency, 'HUBI' as endpoint, 'HUBI-3-S2' AS Account, 'margin_loan' AS BalanceType, Balance as balance, asof, ROW_NUMBER() OVER (PARTITION BY symbol, Currency, Type, CurrencyType ORDER BY asof DESC) as rn 
  from RawData.hubi_3_s2.isolated_margin_loan_balance WITH(NOLOCK) WHERE AsOf BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'  
  UNION
  SELECT symbol as currency, 'HUBI' as endpoint, 'HUBI-3-S2' AS Account, 'Unrealized' AS BalanceType, (profit_unreal) as balance, asof, ROW_NUMBER() OVER (PARTITION BY symbol, contract_code ORDER BY asof DESC) as rn 
  from RawData.hubi_3_s2.swap_account_info WITH(NOLOCK) WHERE AsOf BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT symbol as currency, 'HUBI' as endpoint, 'HUBI-3-S3' AS Account, 'account_info balance' AS BalanceType, (margin_balance) as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) as rn 
  from RawData.hubi_3_s3.contract_account_info WITH(NOLOCK) WHERE timestamp BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT currency as currency, 'HUBI' as endpoint, 'HUBI-3-S3' AS Account, 'margin_loan' AS BalanceType, Balance as balance, asof, ROW_NUMBER() OVER (PARTITION BY symbol, Currency, Type, CurrencyType ORDER BY asof DESC) as rn 
  from RawData.hubi_3_s3.isolated_margin_loan_balance WITH(NOLOCK) WHERE AsOf BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT symbol as currency, 'HUBI' as endpoint, 'HUBI-3-S3' AS Account, 'account_info unrealized' AS BalanceType, (profit_unreal) as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) as rn 
  from RawData.hubi_3_s3.contract_account_info WITH(NOLOCK) WHERE timestamp BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT symbol as currency, 'HUBI' as endpoint, 'FUBI-M' AS Account, '' AS BalanceType, (margin_balance) as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) as rn 
  from RawData.fubi.contract_account_info WITH(NOLOCK) WHERE timestamp BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'  
  UNION
  SELECT symbol as currency, 'HUBI' as endpoint, 'FUBI-M' AS Account, 'Unrealized' AS BalanceType, (profit_unreal) as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) as rn  
  from RawData.fubi.contract_account_info WITH(NOLOCK) WHERE timestamp BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'  
  UNION
  SELECT symbol as currency, 'HUBI' as endpoint, 'FUB2-M' AS Account, '' AS BalanceType, (margin_balance) as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) as rn 
  from RawData.fub2.contract_account_info WITH(NOLOCK) WHERE timestamp BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT symbol as currency, 'HUBI' as endpoint, 'FUB2-M' AS Account, 'Unrealized' AS BalanceType, (profit_unreal) as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) as rn 
  from RawData.fub2.contract_account_info WITH(NOLOCK) WHERE timestamp BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION 
  SELECT ccy as currency, 'OKEX' as endpoint, 'OKEX-2-S1' AS Account, '' AS BalanceType, (cashBal) as balance, asof, ROW_NUMBER() OVER (PARTITION BY ccy ORDER BY asof DESC) as rn 
  from RawData.okex_2_s1.account_balance WITH(NOLOCK) WHERE AsOf BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'  
  UNION
  SELECT ccy as currency, 'OKEX' as endpoint, 'OKEX-2-S1' AS Account, 'Unrealized' AS BalanceType, (upl) as balance, asof, ROW_NUMBER() OVER (PARTITION BY ccy ORDER BY asof DESC) as rn 
  from RawData.okex_2_s1.account_balance WITH(NOLOCK) WHERE AsOf BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'  
  UNION
  SELECT ccy as currency, 'OKEX' as endpoint, 'OKEX-2-S2' AS Account, '' AS BalanceType, (cashBal) as balance, asof, ROW_NUMBER() OVER (PARTITION BY ccy ORDER BY asof DESC) as rn 
  from RawData.okex_2_s2.account_balance WITH(NOLOCK) WHERE AsOf BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT ccy as currency, 'OKEX' as endpoint, 'OKEX-2-S2' AS Account, 'Unrealized' AS BalanceType, (upl) as balance, asof, ROW_NUMBER() OVER (PARTITION BY ccy ORDER BY asof DESC) as rn 
  from RawData.okex_2_s2.account_balance WITH(NOLOCK) WHERE AsOf BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT ccy as currency, 'OKEX' as endpoint, 'OKEX-2-M' AS Account, '' AS BalanceType, (cashBal) as balance, asof, ROW_NUMBER() OVER (PARTITION BY ccy ORDER BY asof DESC) as rn 
  from RawData.okex_2_u1.account_balance WITH(NOLOCK) WHERE AsOf BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT ccy as currency, 'OKEX' as endpoint, 'OKEX-2-M' AS Account, 'Unrealized' AS BalanceType, (upl) as balance, asof, ROW_NUMBER() OVER (PARTITION BY ccy ORDER BY asof DESC) as rn 
  from RawData.okex_2_u1.account_balance WITH(NOLOCK) WHERE AsOf BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT ccy as currency, 'OKEX' as endpoint, 'OKEX-2-M-W' AS Account, 'Balance' AS BalanceType, (bal) as balance, asof, ROW_NUMBER() OVER (PARTITION BY ccy ORDER BY asof DESC) as rn 
  from RawData.okex_2_u1.funding_asset_balance WITH(NOLOCK) WHERE AsOf BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT id as currency, 'FBLK' as endpoint, 'FBLK' AS Account, '' AS BalanceType, sum(total) as balance, asof, ROW_NUMBER() OVER (PARTITION BY id ORDER BY asof DESC) as rn 
  from RawData.Fireblock.vault_accounts_asset WITH(NOLOCK) WHERE AsOf BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}' GROUP BY id, asof
  UNION
/*  SELECT 'USDT' as currency, RawData_asset_store_code as endpoint, RawData_asset_store_code AS Account, 'Margin Loan' AS BalanceType, value as balance, to_timestamp('{from_ts:%Y-%m-%d %H:%M:%S}') as asof, 1 as rn 
  FROM RawData.vw_loans WITH(NOLOCK) WHERE RawData_asset_store_code not in ('FTXE', 'WOOX', 'XRPF', 'OXTF', 'BTGO', 'GACM')
  UNION */
  SELECT token as currency, 'WOOX' as endpoint, 'WOOX-1-M-E' AS Account, '' AS BalanceType, Holding as balance, asof_CST as asof, ROW_NUMBER() OVER (PARTITION BY token ORDER BY asof_CST DESC) as rn  
  from RawData.woox_1_m_e.holdings WITH(NOLOCK) WHERE asof_CST BETWEEN '{from_ts:%Y-%m-%d %H:%M:%S}' AND '{to_ts:%Y-%m-%d %H:%M:%S}'
) as rawdata
WHERE balance <> 0 AND rn= 1
"""

GET_CME_CRYPTO_SETTLEMENT = """
"""


class RawDataRepository:

    def __init__(self, sql_query_driver, db_connector_factory):
        self._sql_query_class = sql_query_driver
        self._db_connector_factory = db_connector_factory

    def adhoc_query(self, sql: str) -> SqlQueryInterface:
        return self._sql_query_class(
            sql,
            db_connector_factory=self._db_connector_factory)

    def get_cme_positions(self, at_dtt_utc: dt.datetime) -> SqlQueryInterface:
        return self._sql_query_class(
            GET_CME_CRYPTO_SETTLEMENT.format(accounts=('UNMBF222', ''), as_of=at_dtt_utc.astimezone(ChicagoTimeZone)),
            db_connector_factory=self._db_connector_factory)

    def get_exchange_balances(self, asof: dt.datetime) -> SqlQueryInterface:
        return self._sql_query_class(
            GET_RAWDATA_EXCHANGE_BALANCES.format(from_ts=asof - dt.timedelta(minutes=180),
                                                 to_ts=asof + dt.timedelta(minutes=5)),
            db_connector_factory=self._db_connector_factory)


GET_COINMARKETCAP_CLOSE_MARKS = """
    select 
        format(timestamp,'yyyyMMdd') as DATE, symbol as Currency, _close as Mark, 'coinmarketcap' as Source
    from 
        RawData.cnmk_1_m.ohlcv_historical
    where 
        format(timestamp,'yyyyMMdd') = '{close_date:%Y%m%d}' and
        name not in ('CyberMiles','Genesis Mana','NFT','NEFTiPEDiA','UNICORN Token','DEONEX COIN','Don-key','LOLTOKEN','SoMee.Social [OLD]')
"""

GET_TRADING_CLOSE_MARKS = """
    select DATE, Currency, Mark, Source 
    from
        trading.pnl.marks
    where DATE = '{close_date:%Y%m%d}'
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
