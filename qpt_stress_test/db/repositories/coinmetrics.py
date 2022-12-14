
import pytz
import pymd
import pandas as pd
import datetime as dt


class CoinmetricsRepo:
    """ Deliver coinmetrics via pymd/clickhouse """

    def __init__(self, sql_query_driver=None, db_connector_factory=None):
        # self._sql_query_class = sql_query_driver or 
        # self._db_connector_factory = db_connector_factory or 
        pymd.init()
        pymd.enable_logging()
        from qpt_stress_test.core.config import ChicagoTimeZone

        tzinfo = pytz.timezone('America/Chicago')

        report_datetime = dt.datetime(2022, 11, 20, 22, 0, 0, 0)
        get_start_datetime = pymd.UtcTimeZone.localize(report_datetime - dt.timedelta(days=2))
        get_end_datetime = pymd.UtcTimeZone.localize(report_datetime)

        #print(report_universe)
        #print(get_start_datetime, get_end_datetime)

    def reference_rate_ts(self, get_symbols, start_datetime, end_datetime) -> pd.Pandas:
        df_index_ranges = pymd.data_access.coinmetrics_reference_rate_supported_tokens()
        df_coinmetrics_reference_rate = pymd.data_access.coinmetrics_reference_rate(get_symbols, start_datetime,
                                                                                    end_datetime,
                                                                                    sampling_timeframe='15 minute',
                                                                                    aggregation_function='max')  # 'avg')
        return pd.DataFrame(df_coinmetrics_reference_rate)
