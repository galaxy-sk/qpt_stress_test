
import datetime as dt

import qpt_stress_test.core.config as config

# Direct connection to QPT databases
import qpt_stress_test.db.repositories.drivers.pyodbc as pyodbc
import qpt_stress_test.db.repositories.qpt_mssql as qpt_mssql
from qpt_stress_test.db.tasks import sv_awoh_dw01_pyodbc_connection_factory


def run(nav_date: dt.date, positions_at_chicago_time: dt.time):
    """ Write raw exchange data to file """
    db_chicago_datetime = config.ChicagoTimeZone.localize(dt.datetime.combine(nav_date, positions_at_chicago_time))
    db_utc_datetime = db_chicago_datetime.astimezone(config.UtcTimeZone)

    exchange_repo = qpt_mssql.RawDataRepository(
                sql_query_driver=pyodbc.SqlQuery, 
                db_connector_factory=sv_awoh_dw01_pyodbc_connection_factory)
    raw_exchange_data_df = exchange_repo.get_exchange_balances(asof=db_chicago_datetime).as_dataframe()

    # Dump to file
    raw_exchange_data_df.to_csv(f"{db_utc_datetime:%Y-%m-%d_%H%M%S}_raw_exchange_data.csv", sep=',')


if __name__ == "__main__":

    # Generate T-1 data; use 4pm Chicago time, as that is what most MS SQL timestamps are:
    run(dt.date.today() - dt.timedelta(days=1), dt.time(hour=16, minute=0, second=0))

    exit()
