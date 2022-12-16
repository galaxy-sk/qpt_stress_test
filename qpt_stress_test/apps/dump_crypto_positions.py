import os, sys
import datetime as dt
import logging

from databricks import sql

import qpt_stress_test.core.config as config
import qpt_stress_test.db.repositories.databricks as databricks
import qpt_stress_test.db.repositories.qpt_pg as qpt_pg
from qpt_stress_test.db.repositories.drivers import sqlalchemy
from qpt_stress_test.db.tasks import bfc_rds_sqlalchemy_engine_factory
from qpt_stress_test.db.repositories.drivers import pyodbc
from qpt_stress_test.db.tasks import sv_awoh_dw01_pyodbc_connection_factory
from qpt_stress_test.db.repositories.drivers import databricks_sql
from qpt_stress_test.db.tasks import gdt_cluster_databricks_connection_factory

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)

fh = logging.FileHandler(filename=f"{__file__.split('.')[0]}.log")
fh.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(ch)
logger.addHandler(fh)


def run(report_date: dt.date):

    trade_repo = qpt_pg.TradingRepository(
            sql_query_driver=sqlalchemy.SqlQuery, 
            db_connector_factory=bfc_rds_sqlalchemy_engine_factory)
    logger.info(f"Running for date {report_date:%Y-%m-%d}")
    last_postions_date = trade_repo.last_positions_date
    logger.info(f"\tLast positions data avaiable for: {trade_repo.last_positions_date:%Y-%m-%d}")

    # Active open contracts
    db_repo = databricks.TradingRepository(
        sql_query_driver=databricks_sql.SqlQuery, 
        db_connector_factory=gdt_cluster_databricks_connection_factory)
    df = db_repo.adhoc_query("WITH ROWS AS (SELECT * FROM qpt.trading_balances_eod LIMIT 2) SELECT * FROM ROWS;").as_dataframe()

    for idx, row in df.iterrows():
        print((idx, row))


if __name__ == "__main__":
    report_date = dt.date.today() - dt.timedelta(days=1)
    run(report_date)
    sys.exit
