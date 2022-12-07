
# -*- coding: utf-8 -*-
"""
"""
import datetime as dt

from .drivers.databricks_sql import SqlQuery
from ..tasks import gdt_cluster_connection
from qpt_stress_test.core.config import ChicagoTimeZone


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


class TradingRepository:

    def __init__(self):
        pass
    
    def adhoc_query(self, sql: str) -> SqlQuery:
        return SqlQuery(sql, databricks_connection_fn=gdt_cluster_connection)

    @property
    def last_positions_date(self) -> dt.date:
        _, data = SqlQuery(GET_LAST_TRADING_BALANCES_EOD_TRADEDATE, databricks_connection_fn=gdt_cluster_connection).as_list()
        return data[0][0].date()

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
