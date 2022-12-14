# Databricks notebook source
# MAGIC %reload_ext autoreload
# MAGIC %autoreload 
# MAGIC import sys, os
# MAGIC project_root_path = os.path.abspath(os.path.join(os.getcwd(), '..'))
# MAGIC if project_root_path not in sys.path:
# MAGIC     sys.path.append(project_root_path)
# MAGIC print(project_root_path)
# MAGIC 
# MAGIC import datetime as dt
# MAGIC from importlib import reload
# MAGIC 
# MAGIC import qpt_stress_test.core.config as config
# MAGIC reload(config)
# MAGIC import qpt_stress_test.db.repositories.databricks as databricks
# MAGIC reload(databricks)
# MAGIC 
# MAGIC print(f"Is this a databricks environment: {config.IS_DATABRICKS_ENVIRON}")
# MAGIC print(config.POSTGRES_URL, config.POSTGRES_JDBC_DRIVER)
# MAGIC print(config.MSSQL_URL, config.MSSQL_JDBC_DRIVER)
# MAGIC 
# MAGIC # trade_service = positions.TradeService(spark)
# MAGIC trading_repo = databricks.TradingRepository(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check SQL Connections

# COMMAND ----------

# Databricks qpt.trading_balances
display(spark.sql(databricks.GET_CLIENT_EOD_TRADING_BALANCES.format(trade_date=dt.date(2022, 11, 23), clients=('CC_TRbtcRateSpread19',''), accounts=('HUBI-E', 'UNMBF222'))))

spark_sql = trading_repo.get_client_eod_trading_balances(trade_date=dt.date(2022, 11, 23), clients=('CC_TRbtcRateSpread19',''), accounts=('HUBI-E', 'UNMBF222'))
display(spark_sql.as_dataframe())

# COMMAND ----------

# Postgres
GET_ACTIVE_CONTRACTS = """
    SELECT * FROM (
        SELECT 
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
        from Trading.definition.instrument_reference 
        where instrument_type in ('SWAP', 'FUTURE') and symbol_bfc is not null and expiration_time >= '{0:%Y-%m-%d 00:00:00.000}' 
    ) inst_ref
"""

df = spark.read \
            .format("jdbc") \
            .option("url", config.POSTGRES_URL) \
            .option("driver", config.POSTGRES_JDBC_DRIVER) \
            .option("query", GET_ACTIVE_CONTRACTS.format(dt.datetime(2022, 11, 1))) \
            .load()
display(df)

# COMMAND ----------

# SQLServer
GET_TRADING_CLOSE_MARKS = """
    select DATE, Currency, Mark, Source 
    from trading.pnl.marks
    where DATE = '{close_date:%Y%m%d}'
"""

df = spark.read \
            .format("jdbc") \
            .option("url", config.MSSQL_URL) \
            .option("driver", config.MSSQL_JDBC_DRIVER) \
            .option("query", GET_TRADING_CLOSE_MARKS.format(close_date=dt.date(2022, 1, 1))) \
            .option("user",  config.MSSQL_USER) \
            .option("password", config.MSSQL_PASSWORD) \
            .load()

display(df)

#    WHERE Client IN {clients} AND Account IN {accounts} AND TYPE = 'BALANCE' AND YYYYMMDD = '{trade_date:%Y%m%d}' 
GET_CLIENT_EOD_TRADING_BALANCES = """
SELECT * FROM (
    SELECT * 
    FROM Trading.pnl.EOD_NEW
    WHERE Client IN {clients} AND Account IN {accounts} AND Symbol = 'USDTHUSD' AND YYYYMMDD = '{trade_date:%Y%m%d}' 
) balances
"""
df = spark.read \
            .format("jdbc") \
            .option("url", config.MSSQL_URL) \
            .option("driver", config.MSSQL_JDBC_DRIVER) \
            .option("query", GET_CLIENT_EOD_TRADING_BALANCES.format(trade_date=dt.date(2022, 11, 23), clients=('CC_TRbtcRateSpread19',''), accounts=('HUBI-E', 'UNMBF222'))) \
            .option("user",  config.MSSQL_USER) \
            .option("password", config.MSSQL_PASSWORD) \
            .load()

display(df)

GET_CLIENT_EOD_TRADING_BALANCES = """
SELECT * FROM (
   SELECT
        a.Account, a.Balance, a.BalanceType, UPPER(a.Currency) as Currency, 
        b.TableName as Source, FORMAT(a.Date_UTC,'yyyyMMdd') as Timestamp, a.AsOf_UTC as Timestamp_Native
   FROM 
       Operations.balances.EndOfDay_00UTC A 
       LEFT JOIN Operations.balances.sources B on a.Account = b.Account
   where 
       balance != 0 
       and a.Date_UTC = '{trade_date:%Y%m%d}' 
       and Currency not like '%SWAP%' 
       and a.Account in {accounts}
) balances
"""
df = spark.read \
            .format("jdbc") \
            .option("url", config.MSSQL_URL) \
            .option("driver", config.MSSQL_JDBC_DRIVER) \
            .option("query", GET_CLIENT_EOD_TRADING_BALANCES.format(trade_date=dt.date(2022, 11, 23), clients=('CC_TRbtcRateSpread19',''), accounts=('HUBI-E', 'UNMBF222'))) \
            .option("user",  config.MSSQL_USER) \
            .option("password", config.MSSQL_PASSWORD) \
            .load()

display(df)

# COMMAND ----------

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

df = spark.read \
            .format("jdbc") \
            .option("url", config.MSSQL_URL) \
            .option("driver", config.MSSQL_JDBC_DRIVER) \
            .option("query", GET_CME_POSITIONS.format(accounts=('UNMBF222', ''),as_of=dt.datetime(2022, 11, 23, 16, 0, 0))) \
            .option("user",  config.MSSQL_USER) \
            .option("password", config.MSSQL_PASSWORD) \
            .load()

display(df)
