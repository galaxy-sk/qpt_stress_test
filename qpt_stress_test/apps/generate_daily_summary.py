import datetime as dt
import pandas as pd

import qpt_stress_test.core.config as config
import qpt_stress_test.core.qpt_config as qpt_config

# Bowen's code:
import qpt_historic_pos.impl.open_positions_report

# Bovas's code:
import nav.utils

# Direct connection to QPT databases
import qpt_stress_test.db.repositories.drivers.pyodbc as pyodbc
import qpt_stress_test.db.repositories.qpt_mssql as qpt_mssql
from qpt_stress_test.db.tasks import sv_awoh_dw01_pyodbc_connection_factory

#
# Lists of accounts are from Bovas NAV code and change regularly; update them on core.config.qpt_config
#

def get_bank_balances_with_fallback(get_date: dt.date):
    balance_date = get_date
    bmo_df, slv_cash, sig_cash = nav.utils.get_bank_balances(balance_date)
    while bmo_df is None:
        balance_date = balance_date - dt.timedelta(days=1)
        bmo_df, slv_cash, sig_cash = nav.utils.get_bank_balances(balance_date)

    if balance_date != get_date:
        print(f'using old cash data from {balance_date:%Y-%m-%d} for {get_date:%Y-%m-%d}')
        df = bmo_df.reset_index()
        df['trade_date'] = f'{get_date:%Y%m%d}'
        df.set_index(['trade_date', 'currency'], inplace=True)
        bmo_df = df

    return bmo_df, slv_cash, sig_cash


def get_edf_cash_with_fallback(get_date: dt.date):
    """ # Running as of today, the accounting file does not exist """
    edf_cash_date = get_date
    while True:
        try:
            edf_cash = nav.utils.get_edf_cash(edf_cash_date, fileloc=qpt_config.datlib_loc)
            break
        except Exception:  #FileNotFoundError:
            # No EDF collateral data for given date
            edf_cash_date = edf_cash_date - dt.timedelta(days=1)

    if edf_cash_date != get_date:
        print(f'using old edf cash data from {edf_cash_date:%Y-%m-%d} for {get_date:%Y-%m-%d}')

    return edf_cash


def generate_daily_nav_00utc(nav_date: dt.date = None) -> tuple:
    """ The logic of this code comes from Bovas' generate_daily_nav_00utc.ipynb notebook """

    # Start with yesterday's data: more likely this exists
    yesterday = nav_date - dt.timedelta(days=1)
    yd_marks_df = nav.utils.get_marks(yesterday)
    yd_assets_df = nav.utils.get_assets(yesterday, qpt_config.exchange_balance_accounts, yd_marks_df)
    yd_loans_df = nav.utils.get_loans(yesterday, qpt_config.loan_accounts, yd_marks_df)
    yd_edf_cash = get_edf_cash_with_fallback(yesterday)
    yd_bmo_df, yd_slv_cash, yd_sig_cash = get_bank_balances_with_fallback(yesterday)
    yd = {
        "day": yesterday,
        'marks': yd_marks_df,
        'assets': yd_assets_df,
        'loans': yd_loans_df,
        'edf': yd_edf_cash,
        'bmo': yd_bmo_df, 
        'slv': yd_slv_cash, 
        'sig': yd_sig_cash
    }

    # Today's data
    marks_df = nav.utils.get_marks(nav_date)
    assets_df = nav.utils.get_assets(nav_date, qpt_config.exchange_balance_accounts, marks_df)
    loans_df = nav.utils.get_loans(nav_date, qpt_config.loan_accounts, marks_df)
    edf_cash = get_edf_cash_with_fallback(nav_date)

    # Get today's bank balances
    td_bmo_df, td_slv_cash, td_sig_cash = get_bank_balances_with_fallback(nav_date)

    td = {
        "day": nav_date,
        'marks': marks_df,
        'assets': assets_df,
        'loans': loans_df,
        'edf': edf_cash,
        'bmo': td_bmo_df, 
        'slv': td_slv_cash, 
        'sig': td_sig_cash
    }

    bs = nav.utils.summarize_bluescales(td, yd, qpt_config.bs_info)
    summary = nav.utils.summarize_all(td, yd, bs)

    # This is what we need for our process
    return assets_df, loans_df, summary


def summary_exchange_balances_00utc(nav_date, assets_df, loans_df, summary):
    """ Replicate the data from the Summary_Exchange_Balances_00UTC workdsheet from Bovas' daily NAV excel workbook """
    # What we need for reporting:
    # compare to td_assets_df_copy
    report_yyyymmdd = f"{nav_date:%Y%m%d}"
    report_timestamp = assets_df.iloc[0]['Timestamp_Native']
    cash_bal = summary.loc['Total Cash in Banks'][report_yyyymmdd]
    edf_bal = summary.loc['EDF'][report_yyyymmdd]
    clearing_bal = summary.loc['Total Balances at Clearing Brokers'][report_yyyymmdd]

    balances_df = pd.concat([assets_df, loans_df], ignore_index=True)

    # Wedbush hack
    if nav_date < dt.date(2022, 12, 13):
        print('here')
    wedbush_amt = 8579966.92 if nav_date < dt.date(2022, 12, 13) else 79941.92 
    balances_df = pd.concat([balances_df, pd.DataFrame.from_dict({
        'Account': ['WEDBUSH'],
        'Balance': [wedbush_amt],
        'BalanceType': [''],
        'Currency': ['USD'],
        'Source': [''],
        'Timestamp': [report_yyyymmdd],
        'Timestamp_Native': [report_timestamp],
        'Notional': [wedbush_amt]
        })], ignore_index=True)

    # 
    balances_df = pd.concat([balances_df, pd.DataFrame.from_dict({
        'Account': ['CASH'],
        'Balance': [cash_bal],
        'BalanceType': ['Balance'],
        'Currency': ['USD'],
        'Source': [''],
        'Timestamp': [report_yyyymmdd],
        'Timestamp_Native': [report_timestamp],
        'Notional': [cash_bal]
        })], ignore_index=True)

    balances_df = pd.concat([balances_df, pd.DataFrame.from_dict({
        'Account': ['ED&F Man Capital'],
        'Balance': [edf_bal],
        'BalanceType': ['Balance'],
        'Currency': ['USD'],
        'Source': [''],
        'Timestamp': [report_yyyymmdd],
        'Timestamp_Native': [report_timestamp],
        'Notional': [edf_bal]
        })], ignore_index=True)

    balances_df[''] = ''

    # Check that BOVAS accounts have a mapping entry
    unconfigured_accounts = [account for account in balances_df['Account'] if account not in qpt_config.account_map]
    if unconfigured_accounts:
        print("These accounts need to be added to account_map")
        for account in unconfigured_accounts:
            print(account)
            qpt_config.account_map[account] = {'TYPE': 'UNKNOWN', 'Endpoint': account.split("-")[0]}
        print()

    balances_df['REFERENCE 1'] = [
        f"{qpt_config.account_map[account]['TYPE']}{qpt_config.account_map[account]['Endpoint']}{qpt_config.account_map[account]['TYPE']}{currency}" 
        for account, currency in zip(balances_df['Account'], balances_df['Currency'])]
    balances_df['REFERENCE 2'] = [
        f"{qpt_config.account_map[account]['TYPE']}{qpt_config.account_map[account]['Endpoint']}" 
        for account in balances_df['Account']]
    balances_df['TYPE'] = [
        f"{qpt_config.account_map[account]['TYPE']}" 
        for account in balances_df['Account']]
    balances_df['Endpoint'] = [
        f"{qpt_config.account_map[account]['Endpoint']}" 
        for account in balances_df['Account']]

    return balances_df


def summary_asset_loans_cash(summary_exchange_balances_df: pd.DataFrame) -> pd.DataFrame:
    # 
    asset_loans_cash_df = pd.DataFrame()

    def instrument_type_map(endpoint, account, balance, balance_type, account_type):
        if balance_type == "Unrealized":
            return "UNREALIZED"
        if account_type == "EXCHLOAN" and account[:4] != "LEND":
            return "MARGINLOAN"
        if endpoint == "GALAXY" and balance > 0:
            return "ASSET"
        return account_type

    asset_loans_cash_df["utc_timestamp"] = summary_exchange_balances_df["Timestamp_Native"]
    asset_loans_cash_df["exchange"] = [
        qpt_config.endpoint_map[endpoint]['exchange']
        for endpoint in summary_exchange_balances_df["Endpoint"]]
    asset_loans_cash_df["account"] = summary_exchange_balances_df["Account"]
    asset_loans_cash_df["instrument"] = summary_exchange_balances_df["Currency"]
    asset_loans_cash_df["position"] = summary_exchange_balances_df["Balance"]
    asset_loans_cash_df["notional"] = summary_exchange_balances_df["Notional"] 
    asset_loans_cash_df["mark_price"] = [
        notional / balance
        for balance, notional in zip(summary_exchange_balances_df["Balance"],
                                     summary_exchange_balances_df["Notional"])]
    asset_loans_cash_df["unrealized_pnl"] = [
        notional if balance_type == "unrealized" else 0 
        for notional, balance_type in zip(summary_exchange_balances_df["Notional"],
                                          summary_exchange_balances_df["BalanceType"])]
    asset_loans_cash_df["instrument_type"] = [
        instrument_type_map(endpoint, account, balance, balance_type, account_type)
        for endpoint, account, balance, balance_type, account_type in zip(
            summary_exchange_balances_df["Endpoint"], summary_exchange_balances_df["Account"], 
            summary_exchange_balances_df["Balance"], 
            summary_exchange_balances_df["BalanceType"], 
            summary_exchange_balances_df["TYPE"])]
    asset_loans_cash_df["expiration_time"] = summary_exchange_balances_df["Timestamp_Native"] 
    asset_loans_cash_df["is_linear"] = True
    asset_loans_cash_df["underlying"] = summary_exchange_balances_df["Currency"]
    return asset_loans_cash_df


def net_open_positions(report_utc_datetime, trading_repo):
    # Run Bowan's net_open_positions_report
    pg_ts_datetime = report_utc_datetime.astimezone(config.ChicagoTimeZone)
    net_open_position_report_df = qpt_historic_pos.impl.open_positions_report.gen_open_position_report(pg_ts_datetime)

    # Add in timestamp
    columns = ['utc_timestamp'] + list(net_open_position_report_df.columns)
    net_open_position_report_df['utc_timestamp'] = report_utc_datetime.replace(tzinfo=None)
    net_open_position_report_df = net_open_position_report_df[columns]

    # Get the CME positions directly from the MSSQL; over weekends, walk backward to get data from past date
    cme_rollback = 0
    cme_df = trading_repo.get_cme_positions(report_utc_datetime).as_dataframe()
    while cme_df.empty and cme_rollback < 6:
        cme_rollback += 1
        cme_df = trading_repo.get_cme_positions(report_utc_datetime - dt.timedelta(days=cme_rollback)).as_dataframe()
    if cme_rollback:
        print(f"Taking CME positions from {report_utc_datetime - dt.timedelta(days=cme_rollback):%Y-%m-%d}")
    if cme_df.empty:
        print(f"No CME positions from {report_utc_datetime - dt.timedelta(days=cme_rollback):%Y-%m-%d}")

    # Add in real as-of timestamp
    cme_df['utc_timestamp'] = report_utc_datetime.replace(tzinfo=None) - dt.timedelta(days=cme_rollback)

    # Remap and transform into a dictionary: {column_name: [values]}
    # df columns ['instrument','exchange','account','position','notional','price','unrealized_pnl','instrument_type',
    #             'expiration_time','is_linear','underlying','EntryCost', 'utc_timestamp']
    cme_positions = {col_name: [] for col_name in cme_df.columns}
    for idx, rs in cme_df.iterrows():
        for column in cme_df.columns:
            cme_positions[column].append(rs[column])
    cme_positions["unrealized_pnl"] = [0 for _ in range(len(cme_positions["unrealized_pnl"]))]
    cme_positions["mark_price"] = cme_positions["price"]
    for col in cme_df.columns:
        if col not in net_open_position_report_df.columns:
            cme_positions.pop(col)

    # Create cme dataframe, rearrange columns to math net open positions, and concat
    cme_df = pd.DataFrame.from_dict(cme_positions)
    # cme_df = cme_df[[net_open_position_report_df.columns]]
    return pd.concat([net_open_position_report_df, cme_df], ignore_index=True)


def generate_nav_and_postions_reports(eod_date: dt.date, report_utc_datetime: dt.datetime, trading_repo, marketdata_repo=None):

    net_open_position_report_df = net_open_positions(report_utc_datetime, trading_repo)

    # Run a facsimile of the Bovas NAV report
    assets_df, loans_df, summary = generate_daily_nav_00utc(eod_date)
    summary_exchange_balances_df = summary_exchange_balances_00utc(eod_date, assets_df, loans_df, summary)

    # Reformat into format to combine nav & deriv positions
    asset_loans_cash_df = summary_asset_loans_cash(summary_exchange_balances_df)
    asset_and_open_positions_df = pd.concat([net_open_position_report_df, asset_loans_cash_df], ignore_index=True)

    # Dump to file
    net_open_position_report_df.to_csv(f"{report_utc_datetime:%Y-%m-%d_%H%M%S}_net_open_positions.csv", sep=',')
    summary_exchange_balances_df.to_csv(f"{eod_date:%Y-%m-%d}_summary_exchange_balances_00utc.csv", sep=',')
    asset_and_open_positions_df.to_csv(f"{report_utc_datetime:%Y-%m-%d_%H%M%S}_asset_and_open_positions.csv", sep=',')

    return net_open_position_report_df, summary_exchange_balances_df, asset_loans_cash_df, asset_and_open_positions_df


def generate_trading_reports(eod_date: dt.date, trading_repo, operations_repo):

    # Group by TradeDate, Endpoint, Account, Product: sum EodPosition
    valid_client_list = [entry['Client'] for entry in trading_repo.get_daily_client_names(eod_date).as_map().values()]
    # another issue; ops and fills have different account lists, and some may be due to name mappings, so union the two
    fills_account_list = [entry['Account'] for entry in trading_repo.get_daily_accounts(eod_date).as_map().values()]
    ops_account_list = [entry['Account'] for entry in operations_repo.get_eod_account_names(eod_date).as_map().values()]
    valid_account_list = sorted(set(fills_account_list + ops_account_list + ['KRKE-E', ]))
    trading_eod_balances = trading_repo.get_eod_balances(eod_date).as_dataframe()

    # Filter out invalid accounts and clients
    valid_trading_balances = trading_eod_balances.loc[
        (trading_eod_balances['Account'].isin(valid_account_list))
        & (trading_eod_balances['Client'].isin(valid_client_list))].reset_index(drop=True)

    # Filter out non-trading clients
    reportable_trading_balances = valid_trading_balances.loc[
        valid_trading_balances['Client'].apply(lambda client: client.startswith("CC_T"))].reset_index(drop=True)

    # Generate product summary
    trading_product_summary = reportable_trading_balances[
        ['TradeDate', 'Product', 'Symbol', 'Endpoint', 'Account', 'EodPosition']] \
        .groupby(['TradeDate', 'Product', 'Symbol', 'Endpoint', 'Account']).sum().reset_index()

    # Generate client summary
    # Make each currency/product into a position column
    trading_position_by_product = reportable_trading_balances.pivot(
        index=['TradeDate', 'Client', 'Endpoint', 'Account', 'Symbol'],
        columns='Product')['EodPosition'].reset_index().rename_axis(None, axis='columns').fillna(0)
    # Group by Client Symbol (remove Endpoint	Account  columns for group_by)
    trading_client_summary_df = trading_position_by_product[
        trading_position_by_product.columns.difference(['Endpoint', 'Account'], sort=False)].groupby(
        ['TradeDate', 'Client', 'Symbol']).sum().reset_index()

    reportable_trading_balances.to_csv(f"{eod_date:%Y-%m-%d}_trading_client_detail.csv", sep=',')
    trading_client_summary_df.to_csv(f"{eod_date:%Y-%m-%d}_trading_client_summary.csv", sep=',')
    trading_product_summary.to_csv(f"{eod_date:%Y-%m-%d}_trading_product_summary.csv", sep=',')


def run(nav_date: dt.date, positions_at_chicago_time: dt.time):

    db_chicago_datetime = config.ChicagoTimeZone.localize(dt.datetime.combine(nav_date, positions_at_chicago_time))
    db_utc_datetime = db_chicago_datetime.astimezone(config.UtcTimeZone)

    trading_repo = qpt_mssql.TradingRepository(sql_query_driver=pyodbc.SqlQuery,
                                               db_connector_factory=sv_awoh_dw01_pyodbc_connection_factory)
    operations_repo = qpt_mssql.OperationsRepository(sql_query_driver=pyodbc.SqlQuery,
                                                     db_connector_factory=sv_awoh_dw01_pyodbc_connection_factory)

    # These are replicating Bovas's NAV numbers, and Bowen's net open positions reports
    generate_nav_and_postions_reports(nav_date, db_utc_datetime,  trading_repo=trading_repo)

    # Also run some trading client reports
    generate_trading_reports(nav_date, trading_repo, operations_repo)

    # Get marks for all positions:
    """pymd.init()
    pymd.enable_logging()
    df_index_ranges = pymd.data_access.coinmetrics_reference_rate_supported_tokens()
    all_symbols = list(df_index_ranges['symbol'].unique())
    first_datetime = pymd.UtcTimeZone.localize(df_index_ranges['start_time'].min().to_pydatetime())
    last_datetime = pymd.UtcTimeZone.localize(df_index_ranges['end_time'].max().to_pydatetime())

    report_datetime = dt.datetime(2022, 11, 20, 22, 0, 0, 0)
    get_start_datetime = pymd.UtcTimeZone.localize(report_datetime - dt.timedelta(days=2))
    get_end_datetime = pymd.UtcTimeZone.localize(pg_ts_datetime)
    symbols = set(asset_and_open_positions_df["instrument"])
    underlying = set(asset_and_open_positions_df["underlying"])
    marketdata_repo.reference_rate_ts(symbols, )"""



if __name__ == "__main__":

    # Bowen's code has some assumptions on location of modules relative to his code:
    import sys
    import os
    if os.path.join(os.getcwd(), "qpt_historic_pos", "impl") not in sys.path:
        print(f"Adding {os.path.join(os.getcwd(), 'qpt_historic_pos', 'impl')} to sys.path")
        sys.path.append(os.path.join(os.getcwd(), "qpt_historic_pos", "impl"))

    # Regenerate T-n to T-1 data; use Chicago time, as that is what most MS SQL timestamps are:
    n = 3
    for day_offset in range(n, 0, -1):
        run(dt.date.today() - dt.timedelta(days=day_offset), dt.time(hour=16, minute=0, second=0))

    # # Regenerate using as of now
    # run(dt.date.today(), dt.datetime.now().astimezone(config.ChicagoTimeZone).time())
   
    exit()
