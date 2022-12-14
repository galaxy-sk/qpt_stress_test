
# -*- coding: utf-8 -*-
"""
"""
import datetime as dt

#from .drivers.databricks_sql import SqlQuery
#from ..tasks import gdt_cluster_connection
from qpt_stress_test.core.config import ChicagoTimeZone

GET_CLIENT_EOD_TRADING_BALANCES = """
    SELECT * 
    FROM qpt.trading_balances_eod 
    WHERE client in {clients} AND Account IN {accounts} AND TradeDate = '{trade_date:%Y-%m-%d}'
    ORDER BY TradeDate DESC, client, symbol, product;
"""

GET_EOD_SPOT_POSITIONS = """
    SELECT
         eod.TradeDate, eod.EndPoint, Account, eod.Symbol, sum(eodposition / qty_multiplier) as position, 0 as notional, 0 as mark, 0 as unrealised_pnl, 
         instrument_type, expiration_time, is_linear, 
         CASE WHEN currency_position is NULL or currency_position = "" THEN currency_settlement ELSE currency_position END AS underlying, qty_multiplier
    FROM 
        qpt.trading_balances_eod eod 
        LEFT JOIN qpt.crypto_instrument_reference i ON i.symbol_bfc = eod.Symbol AND i.endpoint = eod.Endpoint
    WHERE 
        eod.TradeDate = '{0}' 
        AND (expiration_time IS NULL OR expiration_time > '{0}') 
        AND i.instrument_type not in ('FUTURE', 'SWAP')
    GROUP BY 
        TradeDate, eod.EndPoint, Account, eod.Symbol, i.instrument_type, currency_position, currency_settlement, expiration_time, is_linear , qty_multiplier
    HAVING           
        sum(eodposition) <> 0 
    ORDER BY 
        TradeDate DESC, eod.EndPoint, Account, eod.Symbol ;
"""

GET_EOD_DERIV_POSITIONS = """
    SELECT
         eod.TradeDate, eod.EndPoint, Account, eod.Symbol, sum(eodposition / qty_multiplier) AS position, 0 as notional, 0 as mark, 0 as unrealised_pnl, 
         instrument_type, expiration_time, is_linear, 
         CASE WHEN currency_position is NULL or currency_position = "" THEN currency_settlement ELSE currency_position END AS underlying , qty_multiplier
    FROM 
        qpt.trading_balances_eod eod 
        LEFT JOIN qpt.crypto_instrument_reference i ON i.symbol_bfc = eod.Symbol AND i.endpoint = eod.Endpoint
    WHERE 
        eod.TradeDate = '{0}' 
        AND (expiration_time IS NULL OR expiration_time > '{0}')
        AND i.instrument_type in ('FUTURE', 'SWAP')
    GROUP BY 
        TradeDate, eod.EndPoint, Account, eod.Symbol, i.instrument_type, currency_position, currency_settlement, expiration_time, is_linear , qty_multiplier
    HAVING           
        sum(eodposition) <> 0 
    ORDER BY 
        TradeDate DESC, eod.EndPoint, Account, eod.Symbol ;
"""

GET_EOD_POSITIONS = """
    SELECT
         eod.TradeDate, eod.EndPoint, Account, eod.Symbol, sum(eodposition / qty_multiplier) as position, 0 as notional, 0 as mark, 0 as unrealised_pnl, 
         instrument_type, expiration_time, is_linear, 
         CASE WHEN currency_position is NULL or currency_position = "" THEN currency_settlement ELSE currency_position END AS underlying
    FROM 
        qpt.trading_balances_eod eod 
        LEFT JOIN qpt.crypto_instrument_reference i ON i.symbol_bfc = eod.Symbol AND i.endpoint = eod.Endpoint
    WHERE 
        eod.TradeDate = '{0}' 
        AND (expiration_time IS NULL OR expiration_time > '{0}')
    GROUP BY 
        TradeDate, eod.EndPoint, Account, eod.Symbol, i.instrument_type, currency_position, currency_settlement, expiration_time, is_linear , qty_multiplier
    HAVING           
        sum(eodposition) <> 0 
    ORDER BY 
        TradeDate DESC, eod.EndPoint, Account, eod.Symbol ;
"""

GET_BACKTEST_SYMBOLS = """
    WITH SYMBOL_SET AS (
        SELECT eod.EndPoint, eod.Symbol FROM  qpt.trading_balances_eod eod LEFT JOIN qpt.crypto_instrument_reference i ON i.symbol_bfc = eod.Symbol AND i.endpoint = eod.Endpoint 
        WHERE TradeDate = '{0}' AND (expiration_time IS NULL OR expiration_time > '{0}') 
        GROUP BY eod.EndPoint, eod.Symbol HAVING sum(eodposition) <> 0 
    )
    SELECT 
         ss.Endpoint, ss.Symbol as symbol_bfc, i.id, i.exchange, i.symbol_exch, i.symbol_root, i.instrument_type, 
         i.future_type, i.gateway_security_type, i.expiration_time, i.is_linear, i.is_active, i.currency_position, i.currency_quote, 
         i.currency_settlement, i.currency_index, i.contract_size, i.tick_size, i.maximal_order_qty, i.qty_multiplier 
    FROM 
        SYMBOL_SET ss 
        LEFT JOIN  qpt.crypto_instrument_reference i ON i.symbol_bfc = ss.Symbol AND i.endpoint = ss.Endpoint 
    ORDER BY 
        ss.Endpoint, ss.Symbol ;
"""

GET_BACKTEST_SPOT_MARKS = """
    WITH SYMBOL_SET AS (
        SELECT eod.EndPoint, eod.Symbol FROM  qpt.trading_balances_eod eod LEFT JOIN qpt.crypto_instrument_reference i ON i.symbol_bfc = eod.Symbol AND i.endpoint = eod.Endpoint 
        WHERE TradeDate = '2022-10-11T00:00:00.000+0000' AND (expiration_time IS NULL OR expiration_time > '2022-10-11T00:00:00.000+0000') 
        GROUP BY eod.EndPoint, eod.Symbol HAVING sum(eodposition) <> 0
    )
    SELECT
         mark.TradeDate, mark.Exchange, mark.Symbol, mark.ClosingPrice
    FROM 
        SYMBOL_SET ss 
        LEFT JOIN qpt.mark_settle_eod mark ON mark.symbol = ss.Symbol AND mark.Exchange = ss.Endpoint 
        LEFT JOIN qpt.crypto_instrument_reference i ON i.symbol_bfc = ss.Symbol AND i.endpoint = ss.Endpoint
    WHERE 
        mark.TradeDate >= '20200101' AND mark.TradeDate <= '20221011' AND i.instrument_type not in ('FUTURE', 'SWAP')
    ORDER BY 
        mark.TradeDate DESC, mark.Exchange, mark.Symbol ;
"""

GET_BACKTEST_DERIV_MARKS = """
    WITH SYMBOL_SET AS (
        SELECT eod.EndPoint, eod.Symbol FROM  qpt.trading_balances_eod eod LEFT JOIN qpt.crypto_instrument_reference i ON i.symbol_bfc = eod.Symbol AND i.endpoint = eod.Endpoint 
        WHERE TradeDate = '2022-10-11T00:00:00.000+0000' AND (expiration_time IS NULL OR expiration_time > '2022-10-11T00:00:00.000+0000') 
        GROUP BY eod.EndPoint, eod.Symbol HAVING sum(eodposition) <> 0
    )
    SELECT 
         mark.TradeDate, mark.Exchange, mark.Symbol, mark.ClosingPrice 
    FROM 
        SYMBOL_SET ss 
        LEFT JOIN qpt.mark_settle_eod mark ON mark.symbol = ss.Symbol AND mark.Exchange = ss.Endpoint 
        LEFT JOIN qpt.crypto_instrument_reference i ON i.symbol_bfc = ss.Symbol AND i.endpoint = ss.Endpoint 
    WHERE 
        mark.TradeDate >= '20200101' AND mark.TradeDate <= '20221011' AND i.instrument_type in ('FUTURE', 'SWAP') 
    ORDER BY 
        mark.TradeDate DESC, mark.Exchange, mark.Symbol ;
"""

GET_CRYPTO_BACKFILL_POSITIONS = """
    WITH CASH as (
      select Endpoint, symbol, sum(case when side = 'S' then -LastFillQuantity / 1e9 else LastFillQuantity/1e9 end) AS Position 
      from qpt.crypto_backfill 
      where Account in ('{2}') 
        and Endpoint in ('{1}') 
        and YYYYMMDD <= '{0}' 
      group by Endpoint, Symbol) 
    SELECT 
        CASH.*, 
        MARK.*  
    FROM 
        CASH LEFT JOIN  
        qpt.mark_settle_eod MARK ON CASH.Symbol = MARK.Symbol AND Cash.EndPoint = MARK.Exchange AND MARK.TradeDate = '{0}' 
    ORDER BY Cash.Endpoint, Cash.Symbol ;
"""

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
        WHERE date < CONVERT(DATETIME, '{0}') AND Symbol IN (SELECT [Contract] FROM Symbols)  AND Account = 'UNMBF222'
        --WHERE InsertTime < CONVERT(DATETIME, '{0}') AND Symbol IN (SELECT [Contract] FROM Symbols)  AND Account = 'UNMBF222'
        UNION ALL
        SELECT 
            Symbol, Account, 
            LastFillQuantity * CASE SIDE WHEN 'S' THEN -1 ELSE 1 END  AS NetQuantity,
            LastFillQuantity * CASE SIDE WHEN 'S' THEN 0 ELSE 1 END  AS LongQuantity,
            LastFillQuantity * CASE SIDE WHEN 'S' THEN 1 ELSE 0 END  AS ShortQuantity,
            LastFillPrice
        FROM Trading.dbo.backfill WITH(NOLOCK)
        WHERE date < CONVERT(DATETIME, '{0}') AND Symbol IN (SELECT [Contract] FROM Symbols) AND Account = 'UNMBF222' )

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

GET_LAST_TRADING_BALANCES_EOD_TRADEDATE = "SELECT Max(TradeDate) as last_date FROM qpt.trading_balances_eod txn;"

# This is too complicated to run from the databricks remote client
GET_EXCHANGE_BALANCES_BEFORE_TIMESTAMP = """
SELECT * FROM (
  SELECT asset as currency, 'BINE' as endpoint, 'BINE-1-M-M' AS Account, '' AS BalanceType, netAsset as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY Timestamp DESC) as rn 
  FROM qpt.bine_1_m_m_margin_account_asset WHERE timestamp     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}' 
  UNION
  SELECT asset as currency, 'BINE' as endpoint, 'BINE-2-S1-E' AS Account, '' AS BalanceType, (free+locked) as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY Timestamp DESC) as rn 
  FROM qpt.bine_2_s1_e_account_asset WHERE timestamp     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}' 
  UNION
  SELECT asset as currency, 'BINE' as endpoint, 'BINE-2-S1-F' AS Account, '' AS BalanceType, WalletBalance as balance, asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY asof DESC) as rn 
  FROM qpt.bine_2_s1_f_account_asset WHERE AsOf     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT asset as currency, 'BINE' as endpoint, 'BINE-2-S1-F' AS Account, 'Unrealized' AS BalanceType, unrealizedProfit as balance, asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY asof DESC) as rn  
  FROM qpt.bine_2_s1_f_account_asset WHERE AsOf     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT asset as currency, 'BINE' as endpoint, 'BINE-2-S1-M' AS Account, '' AS BalanceType, netAsset as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY Timestamp DESC) as rn 
  FROM qpt.bine_2_s1_m_margin_account_asset WHERE timestamp     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT asset as currency, 'BINE' as endpoint, 'BINE-2-S1-P' AS Account, '' AS BalanceType, WalletBalance as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY Timestamp DESC) as rn 
  FROM qpt.bine_2_s1_p_futures_account_assets WHERE timestamp     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT asset as currency, 'BINE' as endpoint, 'BINE-2-S1-P' AS Account, 'Unrealized' AS BalanceType, unrealizedProfit as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY Timestamp DESC) as rn 
  FROM qpt.bine_2_s1_p_futures_account_assets WHERE timestamp     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT asset as currency, 'BINE' as endpoint, 'BINE-2-S2-E' AS Account, '' AS BalanceType, (free+locked) as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY Timestamp DESC) as rn 
  FROM qpt.bine_2_s2_e_account_asset WHERE timestamp     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT asset as currency, 'BINE' as endpoint, 'BINE-2-S2-F' AS Account, '' AS BalanceType, WalletBalance as balance, asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY asof DESC) as rn 
  FROM qpt.bine_2_s2_f_account_asset WHERE AsOf     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT asset as currency, 'BINE' as endpoint, 'BINE-2-S2-F' AS Account, 'Unrealized' AS BalanceType, unrealizedProfit as balance, asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY asof DESC) as rn 
  FROM qpt.bine_2_s2_f_account_asset WHERE AsOf     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT asset as currency, 'BINE' as endpoint, 'BINE-2-S2-M' AS Account, '' AS BalanceType, netAsset as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY Timestamp DESC) as rn 
  FROM qpt.bine_2_s2_m_margin_account_asset WHERE timestamp     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT asset as currency, 'BINE' as endpoint, 'BINE-2-S2-P' AS Account, '' AS BalanceType, WalletBalance as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY Timestamp DESC) as rn 
  FROM qpt.bine_2_s2_p_futures_account_assets WHERE timestamp     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT asset as currency, 'BINE' as endpoint, 'BINE-2-S2-P' AS Account, 'Unrealized' AS BalanceType, unrealizedProfit as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY Timestamp DESC) as rn  
  FROM qpt.bine_2_s2_p_futures_account_assets WHERE timestamp     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT asset as currency, 'BINE' as endpoint, 'BINE-MX-S1-E' AS Account, '' AS BalanceType, (free+locked) as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY Timestamp DESC) as rn 
  FROM qpt.bine_mx_s1_e_account_asset WHERE timestamp     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT asset as currency, 'BINE' as endpoint, 'BINE-MX-S1' AS Account, '' AS BalanceType, netAsset as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY Timestamp DESC) as rn 
  FROM qpt.bine_mx_s1_m_margin_account_asset WHERE     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT asset as currency, 'BINE' as endpoint, 'BINE-MX-S1-F' AS Account, '' AS BalanceType, WalletBalance as balance, asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY asof DESC) as rn 
  FROM qpt.bine_mx_s1_f_account_asset WHERE AsOf     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT asset as currency, 'BINE' as endpoint, 'BINE-MX-S1-P' AS Account, '' AS BalanceType, WalletBalance as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY Timestamp DESC) as rn 
  FROM qpt.bine_mx_s1_p_futures_account_assets WHERE timestamp     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT asset as currency, 'BINE' as endpoint, 'BINE-MX-S1-P' AS Account, 'Unrealized' AS BalanceType, unrealizedProfit as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY Timestamp DESC) as rn 
  FROM qpt.bine_mx_s1_p_futures_account_assets WHERE timestamp     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT currency as currency, 'BTFX' as endpoint, 'BTFX-1-M-E' AS Account, '' AS BalanceType, balance as balance, asof_CST as asof, ROW_NUMBER() OVER (PARTITION BY wallet_type, currency ORDER BY asof_CST DESC) as rn 
  FROM qpt.btfx_1_m_e_wallet_balance WHERE wallet_type = 'exchange' AND asof_CST     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}' 
  UNION
  SELECT currency as currency, 'BTFX' as endpoint, 'BTFX-1-M-E' AS Account, wallet_type AS BalanceType, balance as balance, asof_CST as asof, ROW_NUMBER() OVER (PARTITION BY wallet_type, currency ORDER BY asof_CST DESC) as rn 
  FROM qpt.btfx_1_m_e_wallet_balance WHERE wallet_type <> 'exchange' AND asof_CST     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT symbol as currency, 'BULL' as endpoint, 'BULL-1-M-E' AS Account, '' AS BalanceType, Total as balance, asof_CST as asof, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY asof_CST DESC) as rn  
  FROM qpt.bull_1_m_spot_account WHERE asof_CST     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT symbol as currencye, 'BULL' as endpoint, 'BULL-2-M-E' AS Account, '' AS BalanceType, Total as balanc, asof_CST as asof, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY asof_CST DESC) as rn 
  FROM qpt.bull_2_m_spot_account WHERE asof_CST     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT symbol as currency, 'BULL' as endpoint, 'BULL-3-M-E' AS Account, '' AS BalanceType, Total as balance, asof_CST as asof, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY asof_CST DESC) as rn 
  FROM qpt.bull_3_m_spot_account WHERE asof_CST     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT symbol as currency, 'BULL' as endpoint, 'BULL-4-M-E' AS Account, '' AS BalanceType, Total as balance, asof_CST as asof, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY asof_CST DESC) as rn 
  FROM qpt.bull_4_m_spot_account WHERE asof_CST     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT currency as currency, bfc_name as endpoint, bfc_name AS Account, '' AS BalanceType, balance as balance, InsertTime as asof, ROW_NUMBER() OVER (PARTITION BY bfc_name, currency ORDER BY InsertTime DESC) as rn 
  FROM qpt.crypto_bank_balances WHERE InsertTime     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT Currency as currency, Exchange as endpoint, Exchange AS Account, '' AS BalanceType, Value as balance, InsertTime as asof, ROW_NUMBER() OVER (PARTITION BY Exchange, Currency ORDER BY InsertTime DESC) as rn 
  FROM qpt.crypto_snapshot_balances where Exchange in ('BTSE', 'KRKE', 'LMAC', 'LMAX') AND InsertTime     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  (SELECT 'usd' as currency, 'DYDX' as endpoint, 'DYDX-1-M' AS Account, '' AS BalanceType, equity as balance, asof, 1 as rn  
  FROM qpt.dydx_1_m_account ORDER BY AsOf DESC LIMIT 1)
  UNION
  SELECT currency as currency, 'HUBI' as endpoint, 'HUBI-3-S1 AB' AS Account, BalanceType, Balance as balance, asof, ROW_NUMBER() OVER (PARTITION BY currency, type, BalanceType ORDER BY AsOf DESC) as rn 
  FROM qpt.hubi_3_s1_account_balance WHERE AsOf     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}' 
  UNION
  SELECT currency as currency, 'HUBI' as endpoint, 'HUBI-3-S2 AB' AS Account, BalanceType, Balance as balance, asof, ROW_NUMBER() OVER (PARTITION BY currency, type, BalanceType ORDER BY AsOf DESC) as rn 
  FROM qpt.hubi_3_s2_account_balance WHERE AsOf     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT currency as currency, 'HUBI' as endpoint, 'HUBI-3-S3-M' AS Account, BalanceType, Balance as balance, asof, ROW_NUMBER() OVER (PARTITION BY currency, type, BalanceType ORDER BY AsOf DESC) as rn 
  FROM qpt.hubi_3_s3_account_balance WHERE AsOf     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT currency as currency, 'HUBI' as endpoint, 'HUB2-M' AS Account, BalanceType, Balance as balance, asof, ROW_NUMBER() OVER (PARTITION BY currency, type, BalanceType ORDER BY AsOf DESC) as rn 
  FROM qpt.hub2_account_balance WHERE AsOf     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION 
  SELECT currency as currency, 'HUBI' as endpoint, 'HUBI-E' AS Account, '' AS BalanceType, sum(Balance) as balance, asof, ROW_NUMBER() OVER (PARTITION BY currency, type, BalanceType ORDER BY AsOf DESC) as rn 
  FROM qpt.hubi_account_balance WHERE AsOf     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}' GROUP BY currency, type, balancetype, asof HAVING type = 'spot' AND balancetype = 'trade' 
  UNION 
  SELECT currency as currency, 'HUBI' as endpoint, 'HUBI-M' AS Account, '' AS BalanceType, sum(Balance) as balance, asof, ROW_NUMBER() OVER (PARTITION BY currency, type, BalanceType ORDER BY AsOf DESC) as rn 
  FROM qpt.hubi_account_balance WHERE AsOf     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}' GROUP BY currency, type, balancetype, asof HAVING type = 'margin' AND balancetype = 'trade' 
  UNION 
  SELECT currency as currency, 'HUBI' as endpoint, 'HUBI-M' AS Account, 'MarginLoan' AS BalanceType, sum(Balance) as balance, asof, ROW_NUMBER() OVER (PARTITION BY currency, type, BalanceType ORDER BY AsOf DESC) as rn 
  FROM qpt.hubi_account_balance WHERE AsOf     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}' GROUP BY currency, type, balancetype, asof HAVING type = 'margin' AND balancetype = 'loan' 
  UNION 
  (SELECT currency as currency, 'HUBI' as endpoint, 'HUBI-E' AS Account, '' AS BalanceType, Balance as balance, asof, ROW_NUMBER() OVER (PARTITION BY currency, type, BalanceType ORDER BY AsOf DESC) as rn 
  FROM qpt.hubi_account_balance WHERE TYPE = 'spot' AND BalanceType = 'trade' AND AsOf     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}' )
  UNION
  SELECT symbol as currency,  'HUBI' as endpoint, 'HUBI-1-M-P' AS Account, '' AS BalanceType, (margin_balance) as balance, asof, ROW_NUMBER() OVER (PARTITION BY symbol, contract_code ORDER BY asof DESC) as rn  
  FROM qpt.hubi_1_m_p_swap_account_info WHERE AsOf     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT symbol as currency,  'HUBI' as endpoint, 'HUBI-1-M-P' AS Account, 'Unrealized' AS BalanceType, (profit_unreal) as balance, asof, ROW_NUMBER() OVER (PARTITION BY symbol, contract_code ORDER BY asof DESC) as rn 
  FROM qpt.hubi_1_m_p_swap_account_info WHERE AsOf     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT symbol as currency, 'HUBI' as endpoint, 'HUBI-3-S1' AS Account, '' AS BalanceType, (margin_balance) as balance, asof, ROW_NUMBER() OVER (PARTITION BY symbol, contract_code ORDER BY asof DESC) as rn 
  FROM qpt.hubi_3_s1_swap_account_info  WHERE AsOf     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT currency as currency, 'HUBI' as endpoint, 'HUBI-3-S1' AS Account, 'margin_loan' AS BalanceType, Balance as balance, asof, ROW_NUMBER() OVER (PARTITION BY symbol, Currency, Type, CurrencyType ORDER BY asof DESC) as rn  
  FROM qpt.hubi_3_s1_isolated_margin_loan_balance WHERE AsOf     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'  
  UNION
  SELECT symbol as currency, 'HUBI' as endpoint, 'HUBI-3-S1' AS Account, 'Unrealized' AS BalanceType, (profit_unreal) as balance, asof, ROW_NUMBER() OVER (PARTITION BY symbol, contract_code ORDER BY asof DESC) as rn 
  FROM qpt.hubi_3_s1_swap_account_info  WHERE AsOf     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT symbol as currency, 'HUBI' as endpoint, 'HUBI-3-S2' AS Account, '' AS BalanceType, (margin_balance) as balance, asof, ROW_NUMBER() OVER (PARTITION BY symbol, contract_code ORDER BY asof DESC) as rn 
  FROM qpt.hubi_3_s2_swap_account_info WHERE  AsOf     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT currency as currency, 'HUBI' as endpoint, 'HUBI-3-S2' AS Account, 'margin_loan' AS BalanceType, Balance as balance, asof, ROW_NUMBER() OVER (PARTITION BY symbol, Currency, Type, CurrencyType ORDER BY asof DESC) as rn 
  FROM qpt.hubi_3_s2_isolated_margin_loan_balance WHERE AsOf     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'  
  UNION
  SELECT symbol as currency, 'HUBI' as endpoint, 'HUBI-3-S2' AS Account, 'Unrealized' AS BalanceType, (profit_unreal) as balance, asof, ROW_NUMBER() OVER (PARTITION BY symbol, contract_code ORDER BY asof DESC) as rn 
  FROM qpt.hubi_3_s2_swap_account_info WHERE AsOf     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT symbol as currency, 'HUBI' as endpoint, 'HUBI-3-S3' AS Account, 'account_info balance' AS BalanceType, (margin_balance) as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) as rn 
  FROM qpt.hubi_3_s3_contract_account_info WHERE timestamp     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT currency as currency, 'HUBI' as endpoint, 'HUBI-3-S3' AS Account, 'margin_loan' AS BalanceType, Balance as balance, asof, ROW_NUMBER() OVER (PARTITION BY symbol, Currency, Type, CurrencyType ORDER BY asof DESC) as rn 
  FROM qpt.hubi_3_s3_isolated_margin_loan_balance WHERE AsOf     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT symbol as currency, 'HUBI' as endpoint, 'HUBI-3-S3' AS Account, 'account_info unrealized' AS BalanceType, (profit_unreal) as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) as rn 
  FROM qpt.hubi_3_s3_contract_account_info WHERE timestamp     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT symbol as currency, 'HUBI' as endpoint, 'FUBI-M' AS Account, '' AS BalanceType, (margin_balance) as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) as rn 
  FROM qpt.fubi_contract_account_info WHERE timestamp     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'  
  UNION
  SELECT symbol as currency, 'HUBI' as endpoint, 'FUBI-M' AS Account, 'Unrealized' AS BalanceType, (profit_unreal) as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) as rn  
  FROM qpt.fubi_contract_account_info WHERE timestamp     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'  
  UNION
  SELECT symbol as currency, 'HUBI' as endpoint, 'FUB2-M' AS Account, '' AS BalanceType, (margin_balance) as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) as rn 
  FROM qpt.fub2_contract_account_info WHERE timestamp     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT symbol as currency, 'HUBI' as endpoint, 'FUB2-M' AS Account, 'Unrealized' AS BalanceType, (profit_unreal) as balance, timestamp as asof, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) as rn 
  FROM qpt.fub2_contract_account_info WHERE timestamp     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT ccy as currency, 'OKEX' as endpoint, 'OKEX-2-S1' AS Account, '' AS BalanceType, (cashBal) as balance, asof, ROW_NUMBER() OVER (PARTITION BY ccy ORDER BY asof DESC) as rn 
  FROM qpt.okex_2_s1_account_balance WHERE AsOf     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'  
  UNION
  SELECT ccy as currency, 'OKEX' as endpoint, 'OKEX-2-S1' AS Account, 'Unrealized' AS BalanceType, (upl) as balance, asof, ROW_NUMBER() OVER (PARTITION BY ccy ORDER BY asof DESC) as rn 
  FROM qpt.okex_2_s1_account_balance WHERE AsOf     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'  
  UNION
  SELECT ccy as currency, 'OKEX' as endpoint, 'OKEX-2-S2' AS Account, '' AS BalanceType, (cashBal) as balance, asof, ROW_NUMBER() OVER (PARTITION BY ccy ORDER BY asof DESC) as rn 
  FROM qpt.okex_2_s2_account_balance WHERE AsOf     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT ccy as currency, 'OKEX' as endpoint, 'OKEX-2-S2' AS Account, 'Unrealized' AS BalanceType, (upl) as balance, asof, ROW_NUMBER() OVER (PARTITION BY ccy ORDER BY asof DESC) as rn 
  FROM qpt.okex_2_s2_account_balance WHERE AsOf     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT ccy as currency, 'OKEX' as endpoint, 'OKEX-2-M' AS Account, '' AS BalanceType, (cashBal) as balance, asof, ROW_NUMBER() OVER (PARTITION BY ccy ORDER BY asof DESC) as rn 
  FROM qpt.okex_2_u1_account_balance WHERE AsOf     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT ccy as currency, 'OKEX' as endpoint, 'OKEX-2-M' AS Account, 'Unrealized' AS BalanceType, (upl) as balance, asof, ROW_NUMBER() OVER (PARTITION BY ccy ORDER BY asof DESC) as rn 
  FROM qpt.okex_2_u1_account_balance WHERE AsOf     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  UNION
  SELECT id as currency, 'FBLK' as endpoint, 'FBLK' AS Account, '' AS BalanceType, sum(total) as balance, asof, ROW_NUMBER() OVER (PARTITION BY id ORDER BY asof DESC) as rn 
  FROM qpt.fireblock_vault_accounts_asset WHERE AsOf     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}' GROUP BY id, asof
  UNION
  SELECT 'USDT' as currency, qpt_asset_store_code as endpoint, qpt_asset_store_code AS Account, 'Margin Loan' AS BalanceType, value as balance, to_timestamp('{0:%Y-%m-%d %H:%M:%S}') as asof, 1 as rn 
  FROM qpt.vw_loans WHERE qpt_asset_store_code not in ('FTXE', 'WOOX', 'XRPF', 'OXTF', 'BTGO', 'GACM')
  UNION
  SELECT token as currency, 'WOOX' as endpoint, 'WOOX-1-M-E' AS Account, '' AS BalanceType, Holding as balance, asof_CST as asof, ROW_NUMBER() OVER (PARTITION BY token ORDER BY asof_CST DESC) as rn  
  FROM qpt.woox_1_m_e_holdings WHERE asof_CST     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}'
  ) 
WHERE balance <> 0 AND rn= 1 ORDER BY endpoint, currency;
"""


GET_EXCHANGE_BALANCES_BEFORE_TIMESTAMP = """
  SELECT ccy as currency, 'OKEX' as endpoint, 'OKEX-2-S2' AS Account, 'Unrealized' AS BalanceType, (upl) as balance, asof, ROW_NUMBER() OVER (PARTITION BY ccy ORDER BY asof DESC) as rn 
  FROM qpt.okex_2_s2_account_balance WHERE AsOf     BETWEEN '{0:%Y-%m-%d %H:%M:%S}' AND '{1:%Y-%m-%d %H:%M:%S}';
"""

class SqlQuery:
    
    def __init__(self, query: str, *params, db_connector_factory=None):
        self._query = query.format(*params)
        self._params = params
        self._spark = db_connector_factory

    def as_dataframe(self):
        df = self._spark.sql(self._query)
        return df

    def as_instrument_map(self) -> dict:
        with self.db_cursor() as cursor:
            cursor.execute(self._query)
            rs = cursor.fetchall()
            column_names = [col[0] for col in cursor.description]
            map = {
                (row.instrument if "instrument" in column_names else row.symbol_bfc).upper(): {
                    column[0]: value
                    for column, value in zip(cursor.description, row)
                }
                for row in rs
            }     
        return map

    def as_map(self) -> dict:
        with self.db_cursor() as cursor:
            cursor.execute(self._query)
            rs = cursor.fetchall()
            rs_as_map = {
                idx: {
                    column[0]: value
                    for column, value in zip(cursor.description, row)
                }
                for idx, row in enumerate(rs)
            }     
        return rs_as_map

    def as_list(self) -> tuple:
        with self.db_cursor() as cursor:
            cursor.execute(self._query)
            rs = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            data = [[value for value in row] for row in rs]
        return columns, data

class TradingRepository:

    def __init__(self, sparksession):
        self._spark = sparksession
    
    def adhoc_query(self, sql: str) -> SqlQuery:
        return SqlQuery(sql, databricks_connection_fn=gdt_cluster_connection)
        return self._spark.sql()
    
    @property
    def last_positions_date(self) -> dt.date:
        _, data = SqlQuery(GET_LAST_TRADING_BALANCES_EOD_TRADEDATE, databricks_connection_fn=gdt_cluster_connection).as_list()
        return data[0][0].date()

    def get_client_eod_trading_balances(self, trade_date, clients, accounts):
        return SqlQuery(GET_CLIENT_EOD_TRADING_BALANCES.format(trade_date=trade_date, clients=tuple(clients), accounts=tuple(accounts)), 
                        db_connector_factory=self._spark)

    def get_exchange_balances(self, asof_utc_timestamp: dt.datetime):
        """ """
        db_timestamp = asof_utc_timestamp.astimezone(ChicagoTimeZone)
        exchange_balance_sql = GET_EXCHANGE_BALANCES_BEFORE_TIMESTAMP.format(db_timestamp + dt.timedelta(minutes=30),
                                                                             db_timestamp)
        return SqlQuery(exchange_balance_sql, databricks_connection_fn=gdt_cluster_connection)

    def get_cme_positions(self, at_dtt_utc: dt.datetime) -> SqlQuery:
        # maybe work out from trade list when we actually started trading this instrument
        return SqlQuery(GET_CME_POSITIONS,
                        at_dtt_utc.astimezone(ChicagoTimeZone).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                        at_dtt_utc.date().strftime('%Y-%m-%d'),
                        databricks_connection_fn=gdt_cluster_connection)
