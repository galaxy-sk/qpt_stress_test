# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook to check Bovas's daily NAV against the database

# COMMAND ----------

# MAGIC %reload_ext autoreload
# MAGIC %autoreload 
# MAGIC import sys, os
# MAGIC project_root_path = os.path.abspath(os.path.join(os.getcwd(), '..'))
# MAGIC if project_root_path not in sys.path:
# MAGIC     sys.path.append(project_root_path)
# MAGIC print(f"Code root path: {project_root_path}")
# MAGIC 
# MAGIC import datetime as dt
# MAGIC import numpy as np
# MAGIC import pandas as pd
# MAGIC import plotly.express as px
# MAGIC 
# MAGIC import pymd 
# MAGIC from qpt_historic_pos.impl.utils.times import ChicagoTimeZone, UtcTimeZone
# MAGIC import qpt_stress_test.core.config as config
# MAGIC import qpt_stress_test.core.qpt_config as qpt_config
# MAGIC import qpt_stress_test.db.repositories.drivers.databricks_spark as databricks_spark
# MAGIC import qpt_stress_test.db.repositories.drivers.jdbc as databricks_jdbc
# MAGIC import qpt_stress_test.db.repositories.qpt_pg as qpt_pg
# MAGIC import qpt_stress_test.db.repositories.qpt_mssql as qpt_mssql
# MAGIC import qpt_stress_test.db.repositories.databricks as databricks
# MAGIC 
# MAGIC from importlib import reload
# MAGIC reload(config)
# MAGIC reload(qpt_config)
# MAGIC reload(databricks_spark)
# MAGIC reload(databricks_jdbc)
# MAGIC reload(databricks)
# MAGIC reload(qpt_pg)
# MAGIC reload(qpt_mssql)
# MAGIC 
# MAGIC print(config.POSTGRES_URL, config.POSTGRES_JDBC_DRIVER)
# MAGIC print(config.MSSQL_URL, config.MSSQL_JDBC_DRIVER)
# MAGIC print(f"IS_DATABRICKS_ENVIRON={config.IS_DATABRICKS_ENVIRON}")
# MAGIC 
# MAGIC pymd.enable_logging()

# COMMAND ----------

# Set dates, etc, for report generation
nav_date = dt.date(2023, 1, 4)
nav_00utc = dt.datetime.combine(nav_date + dt.timedelta(days=1), dt.time(hour=0, minute=0, second=0))
nav_ctz = UtcTimeZone.localize(dt.datetime.combine(nav_date + dt.timedelta(days=1), dt.time(hour=0, minute=0, second=0))).astimezone(ChicagoTimeZone)
derivs_utc_datetime = ChicagoTimeZone.localize(dt.datetime.combine(nav_date, dt.time(hour=16, minute=0, second=0))).astimezone(UtcTimeZone)

# COMMAND ----------

mktdata_repo = qpt_mssql.MarketDataRepository(sql_query_driver=databricks_jdbc.SqlQuery, db_connector_factory=databricks_jdbc.SparkJdbcConnector.qpt_mssql_connector(spark))
operations_repo = qpt_mssql.OperationsRepository(sql_query_driver=databricks_jdbc.SqlQuery, db_connector_factory=databricks_jdbc.SparkJdbcConnector.qpt_mssql_connector(spark))
trading_repo = qpt_mssql.TradingRepository(sql_query_driver=databricks_jdbc.SqlQuery, db_connector_factory=databricks_jdbc.SparkJdbcConnector.qpt_mssql_connector(spark))
exchange_repo = qpt_mssql.RawDataRepository(sql_query_driver=databricks_jdbc.SqlQuery, db_connector_factory=databricks_jdbc.SparkJdbcConnector.qpt_mssql_connector(spark))

db_trading_repo = databricks.TradingRepository(sql_query_driver=databricks_spark.SqlQuery, db_connector_factory=spark)

pg_trading_repo = qpt_pg.TradingRepository(sql_query_driver=databricks_jdbc.SqlQuery, db_connector_factory=databricks_jdbc.SparkJdbcConnector.qpt_pg_connector(spark))
pg_reference_repo = qpt_pg.ReferenceRepository(sql_query_driver=databricks_jdbc.SqlQuery, db_connector_factory=databricks_jdbc.SparkJdbcConnector.qpt_pg_connector(spark))

# COMMAND ----------

# Bovas config
bovas_asset_account_name_replacement = (
    ['OKEX-2-W1', 'OKEX-2-U1', 'FBLK-AUDIT-BINE', 'FBLK-AUDIT-HUBI', 'FBLK-AUDIT-OKEX', 'FBLK-Default', 'FBLK-GOTC-GACM', 'FBLK-LEND-BTGO', 'FBLK-LEND-DRAW', 
     'FBLK-LEND-OXTF', 'FBLK-LEND-XRPF', 'FBLK-LMAC-M', 'FBLK-Network Deposits', 'FBLK-PITX-E', 'FBLK-LEND-CELS', 'FBLK-LEND-GADI', 'FBLK-LEND-NICO', 
     'FBLK-LEND-GENX','FBLK-BINE-MX-S1','FBLK-DEFI-AAVE','FBLK-GOTC-BIGO','FBLK-DYDX-1-M-P','FBLK-WOOX-1-M-E','FBLK-LEND-GACM'], 
    ['OKEX-2-M-W', 'OKEX-2-M', 'FBLK', 'FBLK', 'FBLK', 'FBLK', 'FBLK', 'FBLK', 'FBLK', 'FBLK', 'FBLK', 'FBLK', 'FBLK', 'FBLK','FBLK', 'FBLK', 'FBLK', 'FBLK', 'FBLK', 'FBLK', 
     'FBLK', 'FBLK', 'FBLK', 'FBLK'])

bovas_loan_account_name_replacement = (
    ['HUBI-M','HUB2-M','WOOX-1-M-E','FTXE-1-M-E','OKEX-2-U1','OKEX-2-S3','BINE-2-S1-M'],
    ['HUBI-M Margin Loan','HUB2-M Margin Loan','WOOX-1-M-E Margin Loan','FTXE-1-M-E Margin Loan', 
     'OKEX-2-M Margin Loan','OKEX-2-S3 Margin Loan','BINE-2-S1-M Margin Loan'])

bovas_currency_replacement = (
    ['USDT_ERC20','SRM_LOCKED','BTCUSD','ETHUSD','EOSUSD','LINKUSD','LTCUSD','ATOM.S','DOT.S','KSM.S','FTM_FANTOM','AUSDC_ETH',
     'CVXCRV-F','VARIABLEDEBTCRV','BNB_BSC','ZIL_BSC','EUROC_ETH_F5NG','USDTEST','FRXETH','AAVAUSDC','VARIABLEDEBTAVAUSDT',
     'VARIABLEDEBTUSDT','AURAB-STETH-STABLE-VAULT'],
    ['USDT','SRM','BTC','ETH','EOS','LINK','LTC','ATOM','DOT','KSM','FTM','AUSDC','CVX','CRV','BNB','ZIL','EUROC','USD','ETH',
    'AUSDC','USDT','USDT','ETH'])

# COMMAND ----------

# Marks from QPT MSSQL database:

marks_df = mktdata_repo.get_coinmarketcap_close_marks(nav_date).as_dataframe().toPandas()
# Add in EUR, GBP, etc missing from coinbase
marks2_df = mktdata_repo.get_trading_close_marks(nav_date).as_dataframe().toPandas()
marks_df = pd.concat([marks_df, marks2_df.loc[~marks2_df['Currency'].isin(marks_df.Currency)]], ignore_index=True)
# Drop duplicates
marks_df.Currency = marks_df.Currency.apply(str.upper)
marks_df.drop(marks_df[marks_df.Currency.duplicated(keep="first")].index, axis=0, inplace=True)
# adding coins which are having a different name but essentially same as an existing coin
marks_df.set_index('Currency', inplace=True)
marks_df.loc['AUSDC'] = marks_df.loc['USDC'].values
marks_df.loc['AWETH'] = marks_df.loc['WETH'].values
marks_df.loc['FRXETH'] = marks_df.loc['ETH'].values

# COMMAND ----------

bovas_loans_qry = """
select a.Account, a.Balance, a.BalanceType, UPPER(a.Currency) as Currency, 
    Isnull(b.TableName, '') as Source, FORMAT(a.Date_UTC,'yyyyMMdd') as Timestamp, a.AsOf_UTC as Timestamp_Native
from Operations.balances.EndOfDay_00UTC A 
    LEFT JOIN Operations.balances.sources B on a.Account = b.Account 
where balance != 0 
    and a.Date_UTC = '{date:%Y-%m-%d}' 
    and Currency not like '%SWAP%' 
    and a.Account in {accounts} 
    and balancetype != 'unrealized' 
""".format(date=nav_date, accounts=tuple(qpt_config.loan_accounts))
bovas_loans_df = trading_repo.adhoc_query(bovas_loans_qry).as_dataframe().toPandas()

bovas_assets_qry = """
select a.Account, a.Balance, a.BalanceType, UPPER(a.Currency) as Currency,  
    Isnull(b.TableName, '') as Source, FORMAT(a.Date_UTC,'yyyyMMdd') as Timestamp, a.AsOf_UTC as Timestamp_Native 
from Operations.balances.EndOfDay_00UTC A 
    LEFT JOIN Operations.balances.sources B on a.Account = b.Account 
where balance != 0 and a.Date_UTC = '{date:%Y-%m-%d}' 
    and Currency not like '%SWAP%' 
    and a.Account in {accounts} 
    and Seconds_from_00UTC <= 79259
""".format(date=nav_date, accounts=tuple(qpt_config.exchange_balance_accounts))
bovas_assets_df = trading_repo.adhoc_query(bovas_assets_qry).as_dataframe().toPandas()

# COMMAND ----------

# Operations EOD 
op_eod_balances_df = operations_repo.get_eod_balances(nav_date).as_dataframe().toPandas()
op_eod_balances_df.Currency.replace(*bovas_currency_replacement, inplace=True)
op_eod_balances_df['Notional'] = op_eod_balances_df.Balance.apply(lambda d: float(d)) * marks_df.reindex(op_eod_balances_df.Currency).fillna(0).Mark.values

# COMMAND ----------

crypto_exchange_df = exchange_repo.get_exchange_balances(asof=nav_ctz).as_dataframe().toPandas()
crypto_exchange_df['Notional'] = crypto_exchange_df.Balance * marks_df.reindex(crypto_exchange_df.Currency).fillna(0).Mark.values

# COMMAND ----------

# We should properly map accounts to Endpoint or exchange
bovas_assets_df['Endpoint'] = bovas_assets_df['Account'].apply(lambda ac: str(ac.split('-')[0]) if str(ac.split('-')[0]) != 'LEND' else str(ac.split('-')[1]))
bovas_loans_df['Endpoint'] = bovas_loans_df['Account'].apply(lambda ac: str(ac.split('-')[0]) if str(ac.split('-')[0]) != 'LEND' else str(ac.split('-')[1]))
op_eod_balances_df['Endpoint'] = op_eod_balances_df['Account'].apply(lambda ac: str(ac.split('-')[0]) if str(ac.split('-')[0]) != 'LEND' else str(ac.split('-')[1]))
crypto_exchange_df['Endpoint'] = crypto_exchange_df['Account'].apply(lambda ac: str(ac.split('-')[0]) if str(ac.split('-')[0]) != 'LEND' else str(ac.split('-')[1]))

# Add in bank balances; 
bank_eod_balances_df = trading_repo.get_bank_balances(nav_date).as_dataframe().toPandas()
bank_eod_balances_df['Notional'] = bank_eod_balances_df.Balance * marks_df.reindex(bank_eod_balances_df.Currency).fillna(0).Mark.values
bank_eod_balances_df['Endpoint'] = 'Cash'
bovas_assets_df = pd.concat([bovas_assets_df, bank_eod_balances_df], ignore_index=True)
op_eod_balances_df = pd.concat([op_eod_balances_df, bank_eod_balances_df], ignore_index=True)

# COMMAND ----------

def _format_assets(daytime, df, marks):
#     df.drop(df.loc[(df.Account.isin(['OKEX-2-W1', 'OKEX-2-U1', 'OKEX-2-S1', 'OKEX-2-S2', 'OKEX-2-S3', 'HUBI-1-M-P', 'FUBI-M','FUB2-M','HUBI-3-S3-F'])) 
#                    & (df.BalanceType!='Balance')].index, axis=0, inplace=True) #comment out to include unrealized
    df.drop(df.loc[(df.Account.isin(['HUBI-M','HUB2-M'])) & (df.BalanceType.isin(['Margin Loan','Unrealized']))].index, axis=0, inplace=True)
    df.Account.replace(['OKEX-2-W1', 'OKEX-2-U1', 'FBLK-AUDIT-BINE', 'FBLK-AUDIT-HUBI', 'FBLK-AUDIT-OKEX', 'FBLK-Default', 'FBLK-GOTC-GACM', 'FBLK-LEND-BTGO', 'FBLK-LEND-DRAW', 
                        'FBLK-LEND-OXTF', 'FBLK-LEND-XRPF', 'FBLK-LMAC-M', 'FBLK-Network Deposits', 'FBLK-PITX-E', 'FBLK-LEND-CELS', 'FBLK-LEND-GADI', 'FBLK-LEND-NICO', 
                        'FBLK-LEND-GENX','FBLK-BINE-MX-S1','FBLK-DEFI-AAVE','FBLK-GOTC-BIGO','FBLK-DYDX-1-M-P','FBLK-WOOX-1-M-E','FBLK-LEND-GACM'], 
                       ['OKEX-2-M-W', 'OKEX-2-M', 'FBLK', 'FBLK', 'FBLK', 'FBLK', 'FBLK', 'FBLK', 'FBLK', 'FBLK', 'FBLK', 'FBLK', 'FBLK', 'FBLK','FBLK', 'FBLK', 'FBLK', 'FBLK', 'FBLK', 'FBLK', 
                        'FBLK', 'FBLK', 'FBLK', 'FBLK'], inplace=True)

    df.drop(df.loc[(df.Account.isin(['DEFI-STRAT-4'])) & (~df.BalanceType.isin(['wallet']))].index, axis=0, inplace=True)
    df.drop(df.loc[(df.Account.isin(['DEFI-STRAT-8'])) & (df.BalanceType.isin(['wallet','borrow_token']))].index, axis=0, inplace=True)
    df.drop(df.loc[(df.Account.isin(['WOOX-1-M-E'])) & (df.Balance<0) & (~df.BalanceType.isin(['Unrealized']))].index, axis=0, inplace=True)
    df.drop(df.loc[(df.Account.isin(['FTXE-1-M-E'])) & (df.Balance<0) & (~df.BalanceType.isin(['Unrealized']))].index, axis=0, inplace=True)
    df.drop(df.loc[(df.Account.isin(['OKEX-2-M'])) & (df.Balance<0) & (~df.BalanceType.isin(['Unrealized']))].index, axis=0, inplace=True)
    df.drop(df.loc[(df.Account.isin(['OKEX-2-S3'])) & (df.Balance<0) & (~df.BalanceType.isin(['Unrealized']))].index, axis=0, inplace=True)
    df.drop(df.loc[(df.Account.isin(['BINE-2-S1-M','BINE-2-S2-M'])) & (df.Balance<0) & (~df.BalanceType.isin(['Unrealized']))].index, axis=0, inplace=True)
    df.drop(df.loc[(df.Account.isin(['LEND-GACM'])) & (df.Balance<0) & (~df.BalanceType.isin(['Unrealized']))].index, axis=0, inplace=True)
    df.Currency.replace(['USDT_ERC20','SRM_LOCKED','BTCUSD','ETHUSD','EOSUSD','LINKUSD','LTCUSD','ATOM.S','DOT.S','KSM.S','FTM_FANTOM','AUSDC_ETH',
                         'CVXCRV-F','VARIABLEDEBTCRV','BNB_BSC','ZIL_BSC','EUROC_ETH_F5NG','USDTEST','FRXETH','AAVAUSDC','VARIABLEDEBTAVAUSDT',
                         'VARIABLEDEBTUSDT','AURAB-STETH-STABLE-VAULT'],
                        ['USDT','SRM','BTC','ETH','EOS','LINK','LTC','ATOM','DOT','KSM','FTM','AUSDC','CVX','CRV','BNB','ZIL','EUROC','USD','ETH',
                         'AUSDC','USDT','USDT','ETH'], inplace=True)
    df['Notional'] = df.Balance.apply(lambda d: float(d)) * marks.reindex(df.Currency).fillna(0).Mark.values
    df = _format_fireblocks(df)
    return df

def _format_fireblocks(df):
    temp = df.loc[df.Account=='FBLK',:].copy()
    df.drop(df.loc[df.Account=='FBLK',:].index, axis=0, inplace=True)
    temp = temp.groupby(['Account','Currency','BalanceType','Source','Timestamp','Timestamp_Native']).sum().reset_index()
    df = df.append(temp, ignore_index=True)
    return df

def _format_loans(daytime, df, marks):
    df.drop(df.loc[(df.Account.isin(['HUBI-M','HUB2-M'])) & (df.BalanceType!='Margin Loan')].index, axis=0, inplace=True)
    df.drop(df.loc[(df.Account.isin(['WOOX-1-M-E'])) & (df.Balance>=0)].index, axis=0, inplace=True)
    df.drop(df.loc[(df.Account.isin(['FTXE-1-M-E'])) & (df.Balance>=0)].index, axis=0, inplace=True)
    df.drop(df.loc[(df.Account.isin(['OKEX-2-U1'])) & (df.Balance>=0)].index, axis=0, inplace=True)
    df.drop(df.loc[(df.Account.isin(['OKEX-2-S3'])) & (df.Balance>=0)].index, axis=0, inplace=True)
    df.drop(df.loc[(df.Account.isin(['BINE-2-S1-M','BINE-2-S2-M'])) & (df.Balance>=0)].index, axis=0, inplace=True)
    df.drop(df.loc[(df.Account.isin(['LEND-GACM'])) & (df.Balance>=0)].index, axis=0, inplace=True)
    df.Account.replace(['HUBI-M','HUB2-M','WOOX-1-M-E','FTXE-1-M-E','OKEX-2-U1','OKEX-2-S3','BINE-2-S1-M'],
                       ['HUBI-M Margin Loan','HUB2-M Margin Loan','WOOX-1-M-E Margin Loan','FTXE-1-M-E Margin Loan', \
                        'OKEX-2-M Margin Loan','OKEX-2-S3 Margin Loan','BINE-2-S1-M Margin Loan'], inplace=True)
    df['Notional'] = df.Balance.apply(lambda d: float(d)) * marks.reindex(df.Currency).fillna(0).Mark.values
    return df

# COMMAND ----------

# Display DF of all records not used in Bovas's report
op_eod = op_eod_balances_df.copy()
bovas_assets_eod = bovas_assets_df.copy()
bovas_assets_eod = _format_assets(nav_date, bovas_assets_eod, marks_df)
bovas_loans_eod = bovas_loans_df.copy()
bovas_loans_eod = _format_loans(nav_date, bovas_loans_eod, marks_df)

# Assets - apply the mappings to account names that Bovas's code does
op_eod.loc[((op_eod['Balance'] > 0) | (op_eod['BalanceType'] == 'Unrealized')), 'Account'] \
      = op_eod.loc[((op_eod['Balance'] > 0) | (op_eod['BalanceType'] == 'Unrealized')), 'Account'].replace(*bovas_asset_account_name_replacement)
#bovas_assets_eod.Account.replace(*bovas_asset_account_name_replacement, inplace=True)
#bovas_assets_eod.Currency.replace(*bovas_currency_replacement, inplace=True)

# Loans
op_eod.loc[((op_eod['Balance'] < 0) & (op_eod['BalanceType'] != 'Unrealized')), 'Account'] \
      = op_eod.loc[((op_eod['Balance'] < 0) & (op_eod['BalanceType'] != 'Unrealized')), 'Account'].replace(*bovas_loan_account_name_replacement)
#bovas_loans_eod.Account.replace(*bovas_loan_account_name_replacement, inplace=True)
#bovas_loans_eod.Currency.replace(*bovas_currency_replacement, inplace=True)

# Sum Fireblocks to currency
op_eod = _format_fireblocks(op_eod)

df1 = pd.concat([op_eod, bovas_assets_eod, bovas_loans_eod]).set_index(op_eod.columns.tolist())
df2 = pd.concat([bovas_assets_eod, bovas_loans_eod]).set_index(op_eod.columns.tolist())
bovas_ignored_df = df1.loc[df2.index.symmetric_difference(df1.index)].reset_index()

# Get all the bovas cypto position records, without the account name mangling, into one df
df1 = pd.concat([op_eod, bovas_ignored_df]).set_index(op_eod.columns.tolist())
df2 = bovas_ignored_df.set_index(op_eod.columns.tolist())
bovas_included_df = df1.loc[df2.index.symmetric_difference(df1.index)].reset_index()

bovas_df = pd.concat([bovas_assets_eod, bovas_loans_eod], ignore_index=True)
bovas_df['Unrealized'] = bovas_df.apply(lambda row: row.Notional if row.BalanceType == 'Unrealized' else 0, axis=1)
bovas_df['Loans-funding'] = bovas_df.apply(lambda row: row.Notional if row.BalanceType != 'Unrealized' and row.Notional < 0 else 0, axis=1)
bovas_df['Assets'] = bovas_df.apply(lambda row: row.Notional if  row.BalanceType != 'Unrealized' and row.Notional > 0 else 0, axis=1)
bovas_totals = bovas_df[['Timestamp', 'Endpoint', 'Loans-funding', 'Assets', 'Unrealized', 'Notional']].groupby(by=['Timestamp',]).sum(numeric_only=True).reset_index()
bovas_by_exchange = bovas_df[['Timestamp', 'Endpoint', 'Loans-funding', 'Assets', 'Unrealized', 'Notional']].groupby(by=['Timestamp', 'Endpoint',]).sum(numeric_only=True).reset_index()

op_eod_df = op_eod.copy()
op_eod_df['Unrealized'] = op_eod_df.apply(lambda row: row.Notional if row.BalanceType == 'Unrealized' else 0, axis=1)
op_eod_df['Loans-funding'] = op_eod_df.apply(lambda row: row.Notional if row.BalanceType != 'Unrealized' and row.Notional < 0 else 0, axis=1)
op_eod_df['Assets'] = op_eod_df.apply(lambda row: row.Notional if  row.BalanceType != 'Unrealized' and row.Notional > 0 else 0, axis=1)
op_eod_totals = op_eod_df[['Timestamp', 'Endpoint', 'Loans-funding', 'Assets', 'Unrealized', 'Notional']].groupby(by=['Timestamp',]).sum(numeric_only=True).reset_index()
op_eod_by_exchange = op_eod_df[['Timestamp', 'Endpoint', 'Loans-funding', 'Assets', 'Unrealized', 'Notional']].groupby(by=['Timestamp', 'Endpoint',]).sum(numeric_only=True).reset_index()

crypto_eod_df = crypto_exchange_df.copy()
crypto_eod_df['Unrealized'] = crypto_eod_df.apply(lambda row: row.Notional if row.BalanceType == 'Unrealized' else 0, axis=1)
crypto_eod_df['Loans-funding'] = crypto_eod_df.apply(lambda row: row.Notional if row.BalanceType != 'Unrealized' and row.Notional < 0 else 0, axis=1)
crypto_eod_df['Assets'] = crypto_eod_df.apply(lambda row: row.Notional if  row.BalanceType != 'Unrealized' and row.Notional > 0 else 0, axis=1)
crypto_eod_totals = crypto_eod_df[['Timestamp', 'Endpoint', 'Loans-funding', 'Assets', 'Unrealized', 'Notional']].groupby(by=['Timestamp',]).sum(numeric_only=True).reset_index()
crypto_eod_by_exchange = crypto_eod_df[['Timestamp', 'Endpoint', 'Loans-funding', 'Assets', 'Unrealized', 'Notional']].groupby(by=['Timestamp', 'Endpoint',]).sum(numeric_only=True).reset_index()

# We can't run Bovas's code in Databricks, so we don't have this
# print("Summary from Bovas's code")
# display(report_totals)
# display(report_by_exchange)

print("Summary from bovas's sql")
display(bovas_totals)
display(bovas_by_exchange)

print("Summary raw data exchange balance tables.")
display(crypto_eod_totals)
display(crypto_eod_by_exchange)

print("Summary from ops table - this shows the size of the effect of double counting, etc, that happens if we ignore finance's exclusions.")
display(op_eod_totals)
display(op_eod_by_exchange)
