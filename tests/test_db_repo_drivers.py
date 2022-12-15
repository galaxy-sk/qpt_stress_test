
import datetime as dt
import pytz

import qpt_stress_test.db.repositories.drivers.databricks_sql as databricks_sql
import qpt_stress_test.db.repositories.drivers.pyodbc as pyodbc
import qpt_stress_test.db.repositories.drivers.sqlalchemy as sqlalchemy
import qpt_stress_test.db.repositories.qpt_pg as qpt_pg
import qpt_stress_test.db.repositories.qpt_mssql as qpt_mssql
import qpt_stress_test.db.repositories.databricks as db_trading

from qpt_stress_test.db.tasks import bfc_rds_sqlalchemy_engine_factory, sv_awoh_dw01_pyodbc_connection_factory,  gdt_cluster_databricks_connection_factory


ChicagoTimeZone = pytz.timezone('America/Chicago')


class TestDatabricksDbSelect:

    def test_databricks_qry_as_df(self):
        # Arrange

        # Act
        qry = databricks_sql.SqlQuery("WITH ROWS AS (SELECT * FROM qpt.trading_balances_eod LIMIT 2) SELECT * FROM ROWS;",
                                      db_connector_factory=gdt_cluster_databricks_connection_factory)
        rpt_df = qry.as_dataframe()

        # Assert
        assert not rpt_df.empty

    def test_databricks_qry_as_list(self):
        # Arrange

        # Act
        qry = databricks_sql.SqlQuery("WITH ROWS AS (SELECT * FROM qpt.trading_balances_eod LIMIT 2) SELECT * FROM ROWS;",
                                      db_connector_factory=gdt_cluster_databricks_connection_factory)
        rpt_list = qry.as_list()

        # Assert
        assert rpt_list

    def test_databricks_qry_as_map(self):
        # Arrange

        # Act
        qry = databricks_sql.SqlQuery("WITH ROWS AS (SELECT * FROM qpt.trading_balances_eod LIMIT 2) SELECT * FROM ROWS;",
                                      db_connector_factory=gdt_cluster_databricks_connection_factory)
        rpt_map = qry.as_map()

        # Assert
        assert rpt_map


class TestPgDbSelect:

    def test_sqlalchemy_qry_as_df(self):
        # Arrange
        from_dt = dt.date.today()

        # Act
        qry = sqlalchemy.SqlQuery(qpt_pg.GET_ACTIVE_CONTRACTS, from_dt, db_connector_factory=bfc_rds_sqlalchemy_engine_factory)
        rpt_df = qry.as_dataframe()

        # Assert
        assert not rpt_df.empty

    def test_sqlalchemy_qry_as_list(self):
        # Arrange
        from_dt = dt.date.today()

        # Act
        qry = sqlalchemy.SqlQuery(qpt_pg.GET_ACTIVE_CONTRACTS, from_dt, db_connector_factory=bfc_rds_sqlalchemy_engine_factory)
        rpt_list = qry.as_list()

        # Assert
        assert rpt_list

    def test_sqlalchemy_qry_as_instrument_map(self):
        # Arrange
        from_dt = dt.date.today()

        # Act
        qry = sqlalchemy.SqlQuery(qpt_pg.GET_ACTIVE_CONTRACTS, from_dt, db_connector_factory=bfc_rds_sqlalchemy_engine_factory)
        rpt_map = qry.as_instrument_map()

        # Assert
        assert rpt_map


class TestMSSqlDbSelect:

    def test_pyodbc_qry_as_df(self):
        # Arrange
        at_dtt_utc = dt.datetime(2022, 11, 7, 21, 0, 0)

        # Act
        qry = pyodbc.SqlQuery(qpt_mssql.GET_CME_POSITIONS.format(as_of=at_dtt_utc.astimezone(ChicagoTimeZone), accounts=("UNMBF222", "")), 
                              db_connector_factory=sv_awoh_dw01_pyodbc_connection_factory)
        rpt_df = qry.as_dataframe()

        # Assert
        assert not rpt_df.empty

    def test_pyodbc_qry_as_list(self):
        # Arrange
        at_dtt_utc = dt.datetime(2022, 11, 7, 21, 0, 0)

        # Act
        qry = pyodbc.SqlQuery(qpt_mssql.GET_CME_POSITIONS.format(as_of=at_dtt_utc.astimezone(ChicagoTimeZone), accounts=("UNMBF222", "")), 
                              db_connector_factory=sv_awoh_dw01_pyodbc_connection_factory)
        rpt_list = qry.as_list()

        # Assert
        assert rpt_list

    def test_pyodbc_qry_as_instrument_map(self):
        # Arrange
        at_dtt_utc = dt.datetime(2022, 11, 7, 21, 0, 0)

        # Act
        qry = pyodbc.SqlQuery(qpt_mssql.GET_CME_POSITIONS.format(as_of=at_dtt_utc.astimezone(ChicagoTimeZone), accounts=("UNMBF222", "")), 
                              db_connector_factory=sv_awoh_dw01_pyodbc_connection_factory)
        rpt_map = qry.as_instrument_map()

        # Assert
        assert rpt_map
