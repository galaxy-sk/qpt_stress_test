
import datetime as dt

import qpt_stress_test.db.repositories.databricks as databricks
import qpt_stress_test.db.repositories.qpt_mssql as qpt_mssql
import qpt_stress_test.db.repositories.qpt_pg as qpt_pg


class TestQptPgRepo:

    def test_pg_trading_adhoc_query(self):
        # Arrange
        repo = qpt_pg.TradingRepository()
        sql = (
            "select symbol_bfc from Trading.definition.instrument_reference "
            "where symbol_bfc is not null and expiration_time >= '{0:%Y-%m-%d 00:00:00.000}' limit 10"
        ).format(dt.date.today())

        # Act
        df = repo.adhoc_query(sql).as_dataframe()

        # Assert
        assert not df.empty

    def test_qfl_pg_crypto_positions(self):
        # Arrange
        qpt_pg_reference_repo = qpt_pg.ReferenceRepository()
        from_dt = dt.date.today()

        # Act
        active_contracts_df = qpt_pg_reference_repo.get_active_contracts(from_dt).as_dataframe()
        
        # Assert
        assert not active_contracts_df.empty

    def test_qfl_pg_instruments(self):
        # Arrange
        qpt_pg_reference_repo = qpt_pg.ReferenceRepository()
        symbol = ['G_T_BTC_USDT_SWAP', 'H_P_ETH-USD-SWAP']

        # Act
        active_contracts_df = qpt_pg_reference_repo.get_instruments_details(symbol).as_dataframe()
        
        # Assert
        assert not active_contracts_df.empty


class TestQptMSSqlTradingRepo:

    def test_ms_trading_adhoc_query(self):
        # Arrange
        repo = qpt_mssql.TradingRepository()
        sql = """
DECLARE @T_DATE AS DATE;
SET @T_DATE = '{0:%Y-%m-%d}';
DECLARE @TS_DATE AS DATETIME;
SET @TS_DATE = '{0:%Y-%m-%d %H:%M:%S}';

WITH 
    Symbols([Contract], Exchange, Multiplier, SecurityType, Expiration, CurrencyCode) AS (
		SELECT [Contract], Exchange, Multiplier, SecurityType, Expiration, CurrencyCode
		FROM Trading.dbo.products WITH(NOLOCK)
		WHERE Exchange = 'CME'
		AND SecurityType = 'FUTURE'
		AND Expiration > @TS_DATE),

	Marks(Symbol, Exchange, TradeDate, SettlementPrice, AsOf) AS (
		SELECT Symbol, Exchange, TradeDate, SettlementPrice, AsOf
		FROM trading.crypto.CMEOfficialSettlement WITH(NOLOCK)
		WHERE Symbol in (SELECT [Contract] FROM Symbols) AND TradeDate = @T_DATE),

	Positions(Symbol, Account, NetQuantity, LongQuantity, ShortQuantity, LastFillPrice) AS (
		SELECT 
			Symbol, Account, 
			LastFillQuantity * CASE SIDE WHEN 'S' THEN -1 ELSE 1 END  AS NetQuantity,
			LastFillQuantity * CASE SIDE WHEN 'S' THEN 0 ELSE 1 END  AS LongQuantity,
			LastFillQuantity * CASE SIDE WHEN 'S' THEN 1 ELSE 0 END  AS ShortQuantity,
			LastFillPrice
		FROM Trading.dbo.fills WITH(NOLOCK)
		WHERE date < @TS_DATE AND Symbol IN (SELECT [Contract] from Symbols)  AND Account = 'UNMBF222'
		UNION ALL
		SELECT 
			Symbol, Account, 
			LastFillQuantity * CASE SIDE WHEN 'S' THEN -1 ELSE 1 END  AS NetQuantity,
			LastFillQuantity * CASE SIDE WHEN 'S' THEN 0 ELSE 1 END  AS LongQuantity,
			LastFillQuantity * CASE SIDE WHEN 'S' THEN 1 ELSE 0 END  AS ShortQuantity,
			LastFillPrice
		FROM Trading.dbo.backfill WITH(NOLOCK)
		WHERE date < @TS_DATE AND Symbol IN (SELECT [Contract] from Symbols) AND Account = 'UNMBF222' )

SELECT 
	'ED&F' AS exchange, 
	'UNMBF222' AS account,
	 p.Symbol AS instrument,
	 Position AS position,
	 Position * m.SettlementPrice * CASE WHEN p.Symbol LIKE 'ETH%' THEN 50 WHEN p.Symbol LIKE 'BTC%' THEN 5 ELSE 1 END AS notional,
	 m.SettlementPrice AS price,
	 (Position * m.SettlementPrice - EntryCost) * CASE WHEN p.Symbol LIKE 'ETH%' THEN 50 WHEN p.Symbol LIKE 'BTC%' THEN 5 ELSE 1 END AS unrealized_pnl,
	 'CME FUTURE' AS instrument_type,
	 s.Expiration  AS expiration_time, 
	 1  AS is_linear,
	 LEFT(p.Symbol,3) AS underlying,
	 EntryCost * CASE WHEN p.Symbol LIKE 'ETH%' THEN 50 WHEN p.Symbol LIKE 'BTC%' THEN 5 ELSE 1 END AS EntryCost
FROM 
	(SELECT 
		Symbol, 
		SUM(NetQuantity) AS Position, 
		SUM(LongQuantity) AS LongQuantity, 
		SUM(ShortQuantity) AS ShortQuantity,
		SUM(NetQuantity * LastFillPrice) AS EntryCost 
	FROM Positions 
	GROUP BY Symbol) p
	JOIN Marks m ON p.Symbol = m.Symbol
	JOIN Symbols s ON p.Symbol = s.Contract;
""".format(dt.datetime.combine(dt.date.today() - dt.timedelta(days=4), dt.time(16, 0, 0)))

        # Act
        df = repo.adhoc_query(sql).as_dataframe()

        # Assert
        assert not df.empty

    def test_mssql_coinmarketcap_marks(self):
        # Arrange
        repo = qpt_mssql.TradingRepository()
        close_date = dt.date.today() - dt.timedelta(days=3)

		# Act
        eod_balances_df = repo.get_operations_eod_balances(close_date).as_dataframe()

		# Assert
        assert not eod_balances_df.empty


class TestQptMSSqlMarketDataRepo:

    def test_mssql_coinmarketcap_marks(self):
        # Arrange
        repo = qpt_mssql.MarketDataRepository()
        close_date = dt.date.today() - dt.timedelta(days=3)

		# Act
        marks_df = repo.get_coinmarketcap_close_marks(close_date).as_dataframe()

		# Assert
        assert not marks_df.empty

    def test_mssql_trading_marks(self):
        # Arrange
        repo = qpt_mssql.MarketDataRepository()
        close_date = dt.date.today() - dt.timedelta(days=3)

		# Act
        marks_df = repo.get_trading_close_marks(close_date).as_dataframe()

		# Assert
        assert not marks_df.empty


class TestDatabricksRepo:

    def test_databricks_adhoc_query(self):
        # Arrange
        repo = databricks.TradingRepository()
        sql = "SELECT Max(TradeDate) as last_date FROM qpt.trading_balances_eod txn;"
        
        # Act
        df = repo.adhoc_query(sql).as_dataframe()

        # Assert
        assert not df.empty
