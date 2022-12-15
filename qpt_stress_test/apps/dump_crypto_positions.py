import os, sys
import datetime as dt
import logging

from databricks import sql

from qpt_stress_test.db.repositories.qpt_pg import TradingRepository, ReferenceRepository

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

    trade_repo = TradingRepository(driver=)
    logger.info(f"Running for date {report_date:%Y-%m-%d}")
    last_postions_date = trade_repo.last_positions_date
    logger.info(f"\tLast positions data avaiable for: {trade_repo.last_positions_date:%Y-%m-%d}")

    # Active open contracts
    with sql.connect(server_hostname = "gdt-mo.cloud.databricks.com",       #os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                     http_path       = "/sql/1.0/endpoints/b09bc4bc73bccd24",   #os.getenv("DATABRICKS_HTTP_PATH"),
                     access_token    = "dapi11e376e20349a386f4764df7af20504a") as connection:   #os.getenv("DATABRICKS_TOKEN")

        with connection.cursor() as cursor:
            cursor.execute("WITH ROWS AS (SELECT * FROM qpt.trading_balances_eod LIMIT 2) SELECT * FROM ROWS;")
            result = cursor.fetchall()

        for row in result:
            print(row)

if __name__ == "__main__":
    report_date = dt.date.today() - dt.timedelta(days=1)
    run(report_date)
    sys.exit
