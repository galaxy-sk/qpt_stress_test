
import datetime as dt

import qpt_stress_test.db.repositories.pg_trading as pg_trading


class TestQptPgRepo:

    def test_qfl_pg_crypto_positions(self):
        # Arrange
        qpt_pg_trading_repo = pg_trading.TradingRepository()
        from_dt = dt.date.today()

        # Act
        active_contracts_df = qpt_pg_trading_repo.get_active_contracts(from_dt, from_dt).as_dataframe()
        
        # Assert
        assert not active_contracts_df.empty

