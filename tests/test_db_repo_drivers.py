
import datetime as dt
import pytz

import qpt_stress_test.db.repositories.drivers.databricks_sql as databricks_sql
import qpt_stress_test.db.repositories.drivers.pyodbc as pyodbc
import qpt_stress_test.db.repositories.drivers.sqlalchemy as sqlalchemy
import qpt_stress_test.db.repositories.qpt_pg as qpt_pg
import qpt_stress_test.db.repositories.qpt_mssql as qpt_mssql
import qpt_stress_test.db.repositories.databricks as db_trading

from qpt_stress_test.db.tasks import bfc_rds_pg_engine, qpt_sqlsvr_connection,  gdt_cluster_connection


ChicagoTimeZone = pytz.timezone('America/Chicago')


class TestDatabricksDbSelect:

    def test_databricks_qry_as_df(self):
        # Arrange

        # Act
        qry = databricks_sql.SqlQuery("WITH ROWS AS (SELECT * FROM qpt.trading_balances_eod LIMIT 2) SELECT * FROM ROWS;",
                                      databricks_connection_fn= gdt_cluster_connection)
        rpt_df = qry.as_dataframe()

        # Assert
        assert not rpt_df.empty

    def test_databricks_qry_as_list(self):
        # Arrange

        # Act
        qry = databricks_sql.SqlQuery("WITH ROWS AS (SELECT * FROM qpt.trading_balances_eod LIMIT 2) SELECT * FROM ROWS;",
                                      databricks_connection_fn=gdt_cluster_connection)
        rpt_list = qry.as_list()

        # Assert
        assert rpt_list

    def test_databricks_qry_as_map(self):
        # Arrange

        # Act
        qry = databricks_sql.SqlQuery("WITH ROWS AS (SELECT * FROM qpt.trading_balances_eod LIMIT 2) SELECT * FROM ROWS;",
                                      databricks_connection_fn=gdt_cluster_connection)
        rpt_map = qry.as_map()

        # Assert
        assert rpt_map


class TestPgDbSelect:

    def test_sqlalchemy_qry_as_df(self):
        # Arrange
        from_dt = dt.date.today()

        # Act
        qry = sqlalchemy.SqlQuery(qpt_pg.GET_ACTIVE_CONTRACTS, from_dt, sqlalchemy_engine_fn=bfc_rds_pg_engine)
        rpt_df = qry.as_dataframe()

        # Assert
        assert not rpt_df.empty

    def test_sqlalchemy_qry_as_list(self):
        # Arrange
        from_dt = dt.date.today()

        # Act
        qry = sqlalchemy.SqlQuery(qpt_pg.GET_ACTIVE_CONTRACTS, from_dt, sqlalchemy_engine_fn=bfc_rds_pg_engine)
        rpt_list = qry.as_list()

        # Assert
        assert rpt_list

    def test_sqlalchemy_qry_as_instrument_map(self):
        # Arrange
        from_dt = dt.date.today()

        # Act
        qry = sqlalchemy.SqlQuery(qpt_pg.GET_ACTIVE_CONTRACTS, from_dt, sqlalchemy_engine_fn=bfc_rds_pg_engine)
        rpt_map = qry.as_instrument_map()

        # Assert
        assert rpt_map


class TestMSSqlDbSelect:

    def test_pyodbc_qry_as_df(self):
        # Arrange
        at_dtt_utc = dt.datetime(2022, 11, 7, 21, 0, 0)

        # Act
        qry = pyodbc.SqlQuery(qpt_mssql.GET_CME_POSITIONS, 
                              at_dtt_utc.astimezone(ChicagoTimeZone).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                              at_dtt_utc.date().strftime('%Y-%m-%d'),
                              pyodbc_conn_fn=qpt_sqlsvr_connection)
        rpt_df = qry.as_dataframe()

        # Assert
        assert not rpt_df.empty

    def test_pyodbc_qry_as_list(self):
        # Arrange
        at_dtt_utc = dt.datetime(2022, 11, 7, 21, 0, 0)

        # Act
        qry = pyodbc.SqlQuery(qpt_mssql.GET_CME_POSITIONS, 
                              at_dtt_utc.astimezone(ChicagoTimeZone).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                              at_dtt_utc.date().strftime('%Y-%m-%d'),
                              pyodbc_conn_fn=qpt_sqlsvr_connection)
        rpt_list = qry.as_list()

        # Assert
        assert rpt_list

    def test_pyodbc_qry_as_instrument_map(self):
        # Arrange
        at_dtt_utc = dt.datetime(2022, 11, 7, 21, 0, 0)

        # Act
        qry = pyodbc.SqlQuery(qpt_mssql.GET_CME_POSITIONS, 
                              at_dtt_utc.astimezone(ChicagoTimeZone).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                              at_dtt_utc.date().strftime('%Y-%m-%d'),
                              pyodbc_conn_fn=qpt_sqlsvr_connection)
        rpt_map = qry.as_instrument_map()

        # Assert
        assert rpt_map
