import os, sys

import datetime as dt

import qpt_stress_test.db.repositories.databricks as databricks
import qpt_stress_test.db.repositories.qpt_mssql as qpt_mssql
import qpt_stress_test.db.repositories.qpt_pg as qpt_pg
from qpt_stress_test.db.repositories.drivers import databricks_sql
from qpt_stress_test.db.tasks import gdt_cluster_databricks_connection_factory
from qpt_stress_test.db.repositories.drivers import sqlalchemy
from qpt_stress_test.db.tasks import bfc_rds_sqlalchemy_engine_factory
from qpt_stress_test.db.repositories.drivers import pyodbc
from qpt_stress_test.db.tasks import sv_awoh_dw01_pyodbc_connection_factory


def run():
    db_repo = databricks.TradingRepository(
            sql_query_driver=databricks_sql.SqlQuery, 
            db_connector_factory=gdt_cluster_databricks_connection_factory)
    ms_repo = qpt_mssql.TradingRepository(
            sql_query_driver=pyodbc.SqlQuery, 
            db_connector_factory=sv_awoh_dw01_pyodbc_connection_factory)
    pg_repo = qpt_pg.TradingRepository(
            sql_query_driver=sqlalchemy.SqlQuery, 
            db_connector_factory=bfc_rds_sqlalchemy_engine_factory)
    print("\nChecking QPT MS SQL Data")
    print("=======================")
    print("Last: trading.")
    print(f"Last: trading balances eod date:                {ms_repo.last_positions_date:%Y-%m-%d}.")

    print("\nChecking QPT Pg SQL Data")
    print("=======================")
    print("Last: trading.")
    print(f"Last: trading balances eod date:                {pg_repo.last_positions_date:%Y-%m-%d}.")
   
    print("\nChecking Databricks Data")
    print("=======================")
    print(f"Last: trading balances eod date:                {db_repo.last_positions_date:%Y-%m-%d}.")

if __name__ == "__main__":
    run()
    sys.exit
