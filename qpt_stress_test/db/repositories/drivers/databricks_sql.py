
import pandas as pd

import databricks.sql.client
from collections.abc import Callable
from contextlib import ContextDecorator, contextmanager
from qpt_stress_test.db.tasks import gdt_cluster_databricks_connection_factory
from .interfaces import SqlQueryInterface
#from pyspark.sql import SparkSession

#spark = SparkSession.builder.appName("PC").enableHiveSupport().getOrCreate()
#spark.table('derivatives.'+ccy+'_joint_events').toPandas()

#def example():
#    with databricks.sql.connect(server_hostname = "gdt-mo.cloud.databricks.com",       #os.getenv("DATABRICKS_SERVER_HOSTNAME"),
#                     http_path       = "/sql/1.0/endpoints/b09bc4bc73bccd24",   #os.getenv("DATABRICKS_HTTP_PATH"),
#                     access_token    = "dapi11e376e20349a386f4764df7af20504a") as connection:   #os.getenv("DATABRICKS_TOKEN")
#
#        with connection.cursor() as cursor:
#            cursor.execute("WITH ROWS AS (SELECT * FROM qpt.trading_balances_eod LIMIT 2) SELECT * FROM ROWS;")
#            result = cursor.fetchall()
#
#        for row in result:
#            print(row)


class SqlQuery(SqlQueryInterface):

    @contextmanager
    def db_cursor(self, *args, **kwargs):
        with self._databricks_connection_factory() as connection:   #os.getenv("DATABRICKS_TOKEN")
            with connection.cursor() as cursor:
                yield cursor

    class cursor_context(ContextDecorator):
        def __init__(self):
            self.cursor = None

        def __enter__(self, *args, **kwargs):
            self.cursor = args[0]._databricks_connection_factory().cursor()
            return self

        def __call__(self, func):
            super().__call__(func)
            
        def __exit__(self, *exc):
            self.cursor.close()
            return False

    def __init__(self, query: str, *params, db_connector_factory: Callable[[], databricks.sql.client.Connection] = None):
        self._query = query.format(*params)
        self._params = params
        self._databricks_connection_factory = db_connector_factory or gdt_cluster_databricks_connection_factory

    def as_dataframe(self):
        db_connection=self._databricks_connection_factory()
        df = pd.read_sql(self._query, con=db_connection)
        return df

    def as_instrument_map(self) -> dict:
        with self.db_cursor() as cursor:
            cursor.execute(self._query)
            rs = cursor.fetchall()
            map = {
                (row.instrument if "instrument" in row else row.symbol_bfc).upper(): {
                    column[0]: value
                    for column, value in zip(cursor.description, row)
                }
                for row in rs
            }     
        return map

    def as_map(self) -> dict:
        with self.db_cursor() as cursor:
            cursor.execute(self._query)
            rs = cursor.fetchall()
            map = {
                idx: {
                    column[0]: value
                    for column, value in zip(cursor.description, row)
                }
                for idx, row in enumerate(rs)
            }     
        return map

    def as_list(self) -> tuple:
        with self.db_cursor() as cursor:
            cursor.execute(self._query)
            rs = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            data = [[value for value in row] for row in rs]
        return columns, data
