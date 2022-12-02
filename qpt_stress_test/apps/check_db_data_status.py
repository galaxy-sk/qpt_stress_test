import os, sys

import datetime as dt

import qpt_stress_test.db.repositories.databricks as databricks
import qpt_stress_test.db.repositories.qpt_mssql as qpt_mssql
import qpt_stress_test.db.repositories.qpt_pg as qpt_pg


def run():
    db_repo = databricks.TradingRepository()
    ms_repo = qpt_mssql.TradingRepository()
    pg_repo = qpt_pg.TradingRepository()
    print("\rChecking QPT MS SQL Data")
    print("=======================")
    print("Last: trading.")
    print(f"Last: trading balances eod date:                {ms_repo.last_positions_date:%Y-%m-%d}.")

    print("\rChecking QPT Pg SQL Data")
    print("=======================")
    print("Last: trading.")
    print(f"Last: trading balances eod date:                {pg_repo.last_positions_date:%Y-%m-%d}.")
   
    print("\rChecking Databricks Data")
    print("=======================")
    print(f"Last: trading balances eod date:                {db_repo.last_positions_date:%Y-%m-%d}.")

if __name__ == "__main__":
    run()
    sys.exit
