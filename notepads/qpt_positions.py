# Databricks notebook source
# MAGIC %reload_ext autoreload
# MAGIC %autoreload 
# MAGIC import sys, os
# MAGIC project_root_path = os.path.abspath(os.path.join(os.getcwd(), '..'))
# MAGIC if project_root_path not in sys.path:
# MAGIC     sys.path.append(project_root_path)
# MAGIC print(f"Code root path: {project_root_path}")
# MAGIC 
# MAGIC import datetime as dt
# MAGIC import qpt_stress_test.core.config as config
# MAGIC 
# MAGIC from importlib import reload
# MAGIC reload(config)
# MAGIC 
# MAGIC print(config.POSTGRES_URL, config.POSTGRES_JDBC_DRIVER)
# MAGIC print(config.MSSQL_URL, config.MSSQL_JDBC_DRIVER)
# MAGIC print(f"IS_DATABRICKS_ENVIRON={config.IS_DATABRICKS_ENVIRON}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check connections to all datasources

# COMMAND ----------

#
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

# 'bfc-rds-2.cq7gi4betiom.us-east-2.rds.amazonaws.com'
GET_NET_OPEN_POSITIONS_SNAPSHOT = """
    select t1.exchange, t1.account, t1.snapshot_time, t1.long_positions_notional, t1.short_positions_notional, t1.long_positions, t1.short_positions, t1.mark_prices, t1.long_unrealized_pnls, t1.short_unrealized_pnls
    from rms.exchange_position_snapshot t1
        inner join (
            select account, min(snapshot_time) as min_time
            from rms.exchange_position_snapshot
            where snapshot_time between '{asof_start:%Y-%m-%d %H:%M:%S.000}' and '{asof_end:%Y-%m-%d %H:%M:%S.000}'
            group by account) t2 on t1.account = t2.account and t1.snapshot_time = t2.min_time
"""
df = spark.read \
            .format("jdbc") \
            .option("url", config.POSTGRES_URL) \
            .option("driver", config.POSTGRES_JDBC_DRIVER) \
            .option("query", GET_NET_OPEN_POSITIONS_SNAPSHOT.format(asof_start=dt.datetime(2022, 12, 13, 15, 55, 0, 0), asof_end=dt.datetime(2022, 12, 13, 16, 10, 0, 0))) \
            .option("user",  config.POSTGRES_USER) \
            .option("password", config.POSTGRES_PASSWORD) \
            .load()
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crypto Net Option positions
# MAGIC 
# MAGIC Source of the data is the QPT Postgres database and the internal reporting is done using Bowan's qpt_historic_pos Python code

# COMMAND ----------


