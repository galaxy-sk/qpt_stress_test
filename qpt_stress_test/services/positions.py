# -*- coding: utf-8 -*-
"""

"""
import datetime as dt

from qpt_stress_test.db.repositories.pg_trading import TradingRepository as PgTradingRepository
from qpt_stress_test.db.repositories.ms_trading import TradingRepository as MssqlTradingRepository

class TradeService:

    def __init__(self):
        self._pg_trading_repo = PgTradingRepository()
        self._ms_trading_repo = MssqlTradingRepository()
    
    def active_contracts(self, from_dt: dt.date, to_dt: dt.date):
        # maybe work out from trade list when we actually started tradeing this instrument 
        return self._pg_trading_repo.get_active_contracts(from_dt, to_dt)

    def cme_positions(self, at_utc_dtt: dt.datetime):
        # maybe work out from trade list when we actually started tradeing this instrument 
        return self._ms_trading_repo.get_cme_positions(at_utc_dtt)
