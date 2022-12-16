# -*- coding: utf-8 -*-
"""

"""
import datetime as dt

import qpt_stress_test.db.repositories.qpt_pg as qpt_pg 
import qpt_stress_test.db.repositories.qpt_mssql as qpt_mssql
from qpt_stress_test.db.repositories.drivers import sqlalchemy
from qpt_stress_test.db.tasks import bfc_rds_sqlalchemy_engine_factory
from qpt_stress_test.db.repositories.drivers import pyodbc
from qpt_stress_test.db.tasks import sv_awoh_dw01_pyodbc_connection_factory

class TradeService:

    def __init__(self):
        self._pg_trading_repo = qpt_pg.TradingRepository(
            sql_query_driver=sqlalchemy.SqlQuery, 
            db_connector_factory=bfc_rds_sqlalchemy_engine_factory)
        self._pg_reference_repo = qpt_pg.ReferenceRepository(
            sql_query_driver=sqlalchemy.SqlQuery, 
            db_connector_factory=bfc_rds_sqlalchemy_engine_factory)
        self._ms_trading_repo = qpt_mssql.TradingRepository(
            sql_query_driver=pyodbc.SqlQuery, 
            db_connector_factory=sv_awoh_dw01_pyodbc_connection_factory)
    
    def active_contracts(self, from_dt: dt.date):
        # maybe work out from trade list when we actually started tradeing this instrument 
        return self._pg_reference_repo.get_active_contracts(from_dt)

    def cme_positions(self, at_utc_dtt: dt.datetime):
        # maybe work out from trade list when we actually started tradeing this instrument 
        return self._ms_trading_repo.get_cme_positions(at_utc_dtt)
