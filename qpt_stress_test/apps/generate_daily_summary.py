import datetime as dt
import pandas as pd

from nav.utils import (
    get_marks, get_assets, get_loans, get_edf_cash, get_bank_balances, summarize_bluescales, summarize_all)
from qpt_historic_pos.impl.utils.times import ChicagoTimeZone, UtcTimeZone
from qpt_historic_pos.impl.open_positions_report import gen_open_position_report
import qpt_stress_test.core.qfl_config as config

import qpt_stress_test.db.repositories.databricks as databricks
import qpt_stress_test.db.repositories.qpt_mssql as qpt_mssql
import qpt_stress_test.db.repositories.qpt_pg as qpt_pg


# These lists of accounts are from Bovas NAV code and change regularly; update them on core.config.qpt
datlib_loc = config.datlib_loc
bs_info = config.bs_info
exchanges_balance_accounts = config.exchanges_balance_accounts
loans_accounts = config.loans_accounts

# In 'NAV Report 00UTC.xls!lookup_table' use: 
#  =""""&A2&""": {"""&A$1&""": """&A2&""", """&B$1&""": """&B2&""", """&C$1&""": """&C2&""", """&D$1&""": """&D2&"""},"
# Add: 
#   "ED&F": {"Account": "ED&F", "BalanceType": "", "TYPE": "COLLATERAL", "Endpoint": "ED&F"},
#   "CASH": {"Account": "CASH", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "CASH"},
account_map = {
    "WEDBUSH": {"Account": "WEDBUSH", "BalanceType": "", "TYPE": "COLLATERAL", "Endpoint": "WEDBUSH"},
    "ED&F Man Capital": {"Account": "ED&F Man Capital", "BalanceType": "", "TYPE": "COLLATERAL", "Endpoint": "ED&F"},
    "CASH": {"Account": "CASH", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "CASH"},
    "BTFX-1-M-E": {"Account": "BTFX-1-M-E", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "BTFX "},
    "BTSE": {"Account": "BTSE", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "BTSE"},
    "FTXE-1-M-E": {"Account": "FTXE-1-M-E", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "FTXE"},
    "FUB2-M": {"Account": "FUB2-M", "BalanceType": "Balance", "TYPE": "ASSET", "Endpoint": "HUBI"},
    "FUBI-M": {"Account": "FUBI-M", "BalanceType": "Balance", "TYPE": "ASSET", "Endpoint": "HUBI"},
    "HUB2-E": {"Account": "HUB2-E", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "HUBI"},
    "HUB2-M": {"Account": "HUB2-M", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "HUBI"},
    "HUBI-1-M-P": {"Account": "HUBI-1-M-P", "BalanceType": "Balance", "TYPE": "ASSET", "Endpoint": "HUBI"},
    "HUBI-3-S3-F": {"Account": "HUBI-3-S3-F", "BalanceType": "Balance", "TYPE": "ASSET", "Endpoint": "BSCALE"},
    "HUBI-3-S3-M": {"Account": "HUBI-3-S3-M", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "BSCALE"},
    "HUBI-E": {"Account": "HUBI-E", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "HUBI"},
    "HUBI-M": {"Account": "HUBI-M", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "HUBI"},
    "KRKE": {"Account": "KRKE", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "KRKE"},
    "LMAC": {"Account": "LMAC", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "LMAC"},
    "LMAX": {"Account": "LMAX", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "LMAX"},
    "OKEX-2-S1": {"Account": "OKEX-2-S1", "BalanceType": "Balance", "TYPE": "ASSET", "Endpoint": "OKEX"},
    "OKEX-2-S2": {"Account": "OKEX-2-S2", "BalanceType": "Balance", "TYPE": "ASSET", "Endpoint": "OKEX"},
    "OKEX-2-S3": {"Account": "OKEX-2-S3", "BalanceType": "Balance", "TYPE": "ASSET", "Endpoint": "OKEX"},
    "OKEX-2-M": {"Account": "OKEX-2-M", "BalanceType": "Balance", "TYPE": "ASSET", "Endpoint": "OKEX"},
    "OKEX-2-M-W": {"Account": "OKEX-2-M-W", "BalanceType": "Balance", "TYPE": "ASSET", "Endpoint": "OKEX"},
    "PITX-E": {"Account": "PITX-E", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "PITX"},
    "SHFT-1-W": {"Account": "SHFT-1-W", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "SHFT WALLET"},
    "WOOX-1-M-E": {"Account": "WOOX-1-M-E", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "WOOX"},
    "FBLK": {"Account": "FBLK", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "FBLK"},
    "BULL-1-M-E": {"Account": "BULL-1-M-E", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "BULL"},
    "BULL-1-M-M": {"Account": "BULL-1-M-M", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "BULL"},
    "LEND-GACM": {"Account": "LEND-GACM", "BalanceType": "", "TYPE": "FUND", "Endpoint": "GALAXY"},
    "LEND-HUBI": {"Account": "LEND-HUBI", "BalanceType": "", "TYPE": "EXCHLOAN", "Endpoint": "HUBI"},
    "LEND-LMAC": {"Account": "LEND-LMAC", "BalanceType": "", "TYPE": "EXCHLOAN", "Endpoint": "LMAC"},
    "LEND-OKEX": {"Account": "LEND-OKEX", "BalanceType": "", "TYPE": "EXCHLOAN", "Endpoint": "OKEX"},
    "LEND-OXTF": {"Account": "LEND-OXTF", "BalanceType": "", "TYPE": "LOAN", "Endpoint": "ORCHID"},
    "LEND-PITX": {"Account": "LEND-PITX", "BalanceType": "", "TYPE": "EXCHLOAN", "Endpoint": "PITX"},
    "LEND-XRPF": {"Account": "LEND-XRPF", "BalanceType": "", "TYPE": "LOAN", "Endpoint": "RIPPLE"},
    "WOOX-1-M-E Margin Loan": {
        "Account": "WOOX-1-M-E Margin Loan", "BalanceType": "", "TYPE": "EXCHLOAN", "Endpoint": "WOOX"},
    "HUBI-M Margin Loan": { 
        "Account": "HUBI-M Margin Loan", "BalanceType": "Margin Loan", "TYPE": "EXCHLOAN", "Endpoint": "HUBI"},
    "HUB2-M Margin Loan": { 
        "Account": "HUB2-M Margin Loan", "BalanceType": "Margin Loan", "TYPE": "EXCHLOAN", "Endpoint": "HUBI"},
    "HUBI-3-S3-M Loan": { 
        "Account": "HUBI-3-S3-M Loan", "BalanceType": "", "TYPE": "EXCHLOAN", "Endpoint": "BSCALE"},
    "FTXE-1-M-E Margin Loan": { 
        "Account": "FTXE-1-M-E Margin Loan", "BalanceType": "", "TYPE": "EXCHLOAN", "Endpoint": "FTXE"},
    "LEND-HUBS6": {"Account": "LEND-HUBS6", "BalanceType": "", "TYPE": "EXCHLOAN", "Endpoint": "HUBI"},
    "OKEX-2-M Margin Loan": { 
        "Account": "OKEX-2-M Margin Loan", "BalanceType": "", "TYPE": "EXCHLOAN", "Endpoint": "OKEX"},
    "BINE-MX-S1-P": {"Account": "BINE-MX-S1-P", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "BINEMX"},
    "BINE-MX-S1-F": {"Account": "BINE-MX-S1-F", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "BINEMX"},
    "BINE-MX-S1-E": {"Account": "BINE-MX-S1-E", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "BINEMX"},
    "BINE-MX-S1-M": {"Account": "BINE-MX-S1-M", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "BINEMX"},
    "DYDX-1-M-P": {"Account": "DYDX-1-M-P", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "DYDX"},
    "BULL-1-M-M Loan": {"Account": "BULL-1-M-M Loan", "BalanceType": "", "TYPE": "EXCHLOAN", "Endpoint": "BULL"},
    "HUBI-3-S3-E": {"Account": "HUBI-3-S3-E", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "BSCALE"},
    "BINE-2-S1-E": {"Account": "BINE-2-S1-E", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "BINE"},
    "BINE-2-S1-F": {"Account": "BINE-2-S1-F", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "BINE"},
    "BINE-2-S1-M": {"Account": "BINE-2-S1-M", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "BINE"},
    "BINE-2-S1-P": {"Account": "BINE-2-S1-P", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "BINE"},
    "BULL-2-M-E": {"Account": "BULL-2-M-E", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "BULL"},
    "BULL-2-M-M": {"Account": "BULL-2-M-M", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "BULL"},
    "LEND-BULL": {"Account": "LEND-BULL", "BalanceType": "", "TYPE": "EXCHLOAN", "Endpoint": "BULL"},
    "HUBI-1-M-PT": {"Account": "HUBI-1-M-PT", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "HUBI"},
    "BULL-3-M-E": {"Account": "BULL-3-M-E", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "BULL"},
    "OKEX-2-S3 Margin Loan": { 
        "Account": "OKEX-2-S3 Margin Loan", "BalanceType": "", "TYPE": "EXCHLOAN", "Endpoint": "OKEX"},
    "BULL-4-M-E": { 
        "Account": "BULL-4-M-E", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "BULL"},
    "BINE-2-S1-M Margin Loan": { 
        "Account": "BINE-2-S1-M Margin Loan", "BalanceType": "", "TYPE": "EXCHLOAN", "Endpoint": "BINE"},
    "BULL-2-M-M Loan": { 
        "Account": "BULL-2-M-M Loan", "BalanceType": "", "TYPE": "EXCHLOAN", "Endpoint": "BULL"},
    "GATE-1-M-E": {"Account": "GATE-1-M-E", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "GATE"},
    "GATE-1-M-M": {"Account": "GATE-1-M-M", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "GATE"},
    "GATE-1-M-M Loan": {"Account": "GATE-1-M-M Loan", "BalanceType": "", "TYPE": "EXCHLOAN", "Endpoint": "GATE"},
    "DEFI-STRAT-1": {"Account": "DEFI-STRAT-1", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "DEFI"},
    "DEFI-STRAT-1 Loan": {"Account": "DEFI-STRAT-1 Loan", "BalanceType": "", "TYPE": "EXCHLOAN", "Endpoint": "DEFI"},
    "BINE-2-S2-E": {"Account": "BINE-2-S2-E", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "BINE"},
    "BINE-2-S2-F": {"Account": "BINE-2-S2-F", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "BINE"},
    "BINE-2-S2-M": {"Account": "BINE-2-S2-M", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "BINE"},
    "BINE-2-S2-P": {"Account": "BINE-2-S2-P", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "BINE"},
    "BINE-2-S2-M Margin Loan": { 
        "Account": "BINE-2-S2-M Margin Loan", "BalanceType": "", "TYPE": "EXCHLOAN", "Endpoint": "BINE"},
    "DEFI-STRAT-2": {"Account": "DEFI-STRAT-2", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "DEFI"},
    "DEFI-STRAT-3": {"Account": "DEFI-STRAT-3", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "DEFI"},
    "DEFI-STRAT-4": {"Account": "DEFI-STRAT-4", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "DEFI"},
}

endpoint_map = {
    "BINE": {"exchange": "BINANCE", },
    "BSCALE": {"exchange": "BSCALE", },
    "BTFX ": {"exchange": "BTFX", },
    "BTSE": {"exchange": "BTSE", },
    "BULL": {"exchange": "BULL", },
    "CASH": {"exchange": "CASH", },
    "CME": {"exchange": "CME", },
    "DEFI": {"exchange": "DEFI", },
    "DYDX": {"exchange": "DYDX", },
    "ED&F": {"exchange": "ED&F", },
    "ED&F Man Capital": {"exchange": "ED&F", },
    "FBLK": {"exchange": "FBLK", },
    "FTXE": {"exchange": "FTX", },
    "FUBI": {"exchange": "HUOBI", },
    "GALAXY": {"exchange": "GALAXY", },
    "GATE": {"exchange": "GATE", },
    "HUBI": {"exchange": "HUOBI", },
    "KRKE": {"exchange": "KRKE", },
    "LMAC": {"exchange": "LMAC", },
    "LMAX": {"exchange": "LMAX", },
    "OKEX": {"exchange": "OKEX", },
    "ORCHID": {"exchange": "ORCHID", },
    "PITX": {"exchange": "PITX", },
    "RIPPLE": {"exchange": "RIPPLE", },
    "SHFT WALLET": {"exchange": "SHFT WALLET", },
    "WOOX": {"exchange": "WOOX", },
    "WEDBUSH": {"exchange": "WEDBUSH", },
}


def generate_daily_nav_00utc(nav_date: dt.date = None) -> tuple:
    """ The logic of this code comes from Bovas' generate_daily_nav_00utc.ipynb notebook """

    def get_bank_balances_with_fallback(get_date: dt.date):
        bank_today = get_date
        bmo_df, slv_cash, sig_cash = get_bank_balances(bank_today)
        while bmo_df is None:
            bank_today = bank_today - dt.timedelta(days=1)
            bmo_df, slv_cash, sig_cash = get_bank_balances(bank_today)

        if bank_today != get_date:
            print(f'using old cash data from {bank_today:%Y-%m-%d} for {get_date:%Y-%m-%d}')
            df = bmo_df.reset_index()
            df['trade_date'] = f'{nav_date:%Y%m%d}'
            df.set_index(['trade_date', 'currency'], inplace=True)
            bmo_df = df

        return bmo_df, slv_cash, sig_cash

    # Start with yesterday's data: more likely this exists
    yesterday = nav_date - dt.timedelta(days=1)
    yd_marks_df = get_marks(yesterday)
    yd_assets_df = get_assets(yesterday, exchanges_balance_accounts, yd_marks_df)
    yd_loans_df = get_loans(yesterday, loans_accounts, yd_marks_df)
    yd_edf_cash = get_edf_cash(yesterday, fileloc=datlib_loc)
    yd_bmo_df, yd_slv_cash, yd_sig_cash = get_bank_balances(yesterday)
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
    marks_df = get_marks(nav_date)
    assets_df = get_assets(nav_date, exchanges_balance_accounts, marks_df)
    loans_df = get_loans(nav_date, loans_accounts, marks_df)
    try:
        edf_cash = get_edf_cash(nav_date, fileloc=datlib_loc)
    except FileNotFoundError:
        print(f"No EDF collateral data: reuse data from {yesterday:%Y-%m-%d}")
        edf_cash = yd_edf_cash
    
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

    bs = summarize_bluescales(td, yd, bs_info)
    summary = summarize_all(td, yd, bs)

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

    balances_df = pd.concat([assets_df, loans_df])

    # Webush hack
    wedbush_amt = 8579966.92
    balances_df = balances_df.append({
        'Account': 'WEDBUSH', 
        'Balance': wedbush_amt, 
        'BalanceType': '', 
        'Currency': 'USD', 
        'Source': '', 
        'Timestamp': report_yyyymmdd, 
        'Timestamp_Native': report_timestamp, 
        'Notional': wedbush_amt
        }, ignore_index=True)

    # 
    balances_df = balances_df.append({
        'Account': 'CASH', 
        'Balance': cash_bal, 
        'BalanceType': 'Balance', 
        'Currency': 'USD', 
        'Source': '', 
        'Timestamp': report_yyyymmdd, 
        'Timestamp_Native': report_timestamp, 
        'Notional': cash_bal
        }, ignore_index=True)
    balances_df = balances_df.append({
        'Account': 'ED&F Man Capital', 
        'Balance': edf_bal, 
        'BalanceType': 'Balance', 
        'Currency': 'USD', 
        'Source': '', 
        'Timestamp': report_yyyymmdd, 
        'Timestamp_Native': report_timestamp, 
        'Notional': edf_bal
        }, ignore_index=True)

    balances_df[''] = ''
    balances_df['REFERENCE 1'] = [
        f"{account_map[account]['TYPE']}{account_map[account]['Endpoint']}{account_map[account]['TYPE']}{currency}" 
        for account, currency in zip(balances_df['Account'], balances_df['Currency'])]
    balances_df['REFERENCE 2'] = [
        f"{account_map[account]['TYPE']}{account_map[account]['Endpoint']}" 
        for account in balances_df['Account']]
    balances_df['TYPE'] = [
        f"{account_map[account]['TYPE']}" 
        for account in balances_df['Account']]
    balances_df['Endpoint'] = [
        f"{account_map[account]['Endpoint']}" 
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

    asset_loans_cash_df["exchange"] = [
        endpoint_map[endpoint]['exchange']
        for endpoint in summary_exchange_balances_df["Endpoint"]]
    asset_loans_cash_df["account"] = summary_exchange_balances_df["Account"]
    asset_loans_cash_df["instrument"] = summary_exchange_balances_df["Currency"]
    asset_loans_cash_df["position"] = summary_exchange_balances_df["Balance"]
    asset_loans_cash_df["notional"] = summary_exchange_balances_df["Notional"] 
    asset_loans_cash_df["mark_price"] = [
        notional / balance
        for balance, notional in zip(summary_exchange_balances_df["Balance"], summary_exchange_balances_df["Notional"])]
    asset_loans_cash_df["unrealized_pnl"] = [
        notional if balance_type == "unrealized" else 0 
        for notional, balance_type in zip(summary_exchange_balances_df["Notional"], summary_exchange_balances_df["BalanceType"])] 
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


def run(eod_date: dt.date, db_chicago_datetime: dt.datetime, trading_repo, marketdata_repo):
    # Run a facimile of the Bovas NAV report
    assets_df, loans_df, summary = generate_daily_nav_00utc(eod_date)
    summary_exchange_balances_df = summary_exchange_balances_00utc(eod_date, assets_df, loans_df, summary)

    # Run Bowan's net_open_positions_report
    net_open_positions_df = gen_open_position_report(db_chicago_datetime)

    # Get the CME positions directly from the MSSQL
    cme_df = trading_repo.get_cme_positions(db_chicago_datetime.astimezone(UtcTimeZone)).as_dataframe()
    # 'instrument','exchange','account','position','notional','price','unrealized_pnl','instrument_type',
    # 'expiration_time','is_linear','underlying','EntryCost'
    cme_positions = [{
        "exchange": str(rs['exchange']),
        "account": str(rs['account']),
        "instrument": str(rs['instrument']),
        "position": float(rs['position']),
        "notional": float(rs['notional']),
        "mark_price": float(rs['price']),
        "unrealized_pnl": float(0),
        "instrument_type": str(rs['instrument_type']),
        "expiration_time": rs['expiration_time'],
        "is_linear": int(rs['is_linear']),
        "underlying": str(rs['underlying'])
    } for idx, rs in cme_df.iterrows()]

    for cme_position_dict in cme_positions:
        net_open_positions_df = net_open_positions_df.append(cme_position_dict, ignore_index=True)

    # Asset & Open positions
    summary_asset_loans_cash_df = summary_asset_loans_cash(summary_exchange_balances_df)
    asset_and_open_positions_df = pd.concat([net_open_positions_df, summary_asset_loans_cash_df])

    # Get marks for all positions:
    report_datetime = dt.datetime(2022, 11, 20, 22, 0, 0, 0)
    get_start_datetime = pymd.UtcTimeZone.localize(report_datetime - dt.timedelta(days=2))
    get_end_datetime = UtcTimeZone.localize(db_chicago_datetime)
    symbols = set(asset_and_open_positions_df["instrument"])
    underlying = set(asset_and_open_positions_df["underlying"])
    marketdata_repo.reference_rate_ts(symbols, )
    return net_open_positions_df, summary_exchange_balances_df, summary_asset_loans_cash_df, asset_and_open_positions_df


pymd.init()
pymd.enable_logging()
df_index_ranges = pymd.data_access.coinmetrics_reference_rate_supported_tokens()
all_symbols = list(df_index_ranges['symbol'].unique())
first_datetime = pymd.UtcTimeZone.localize(df_index_ranges['start_time'].min().to_pydatetime())
last_datetime = pymd.UtcTimeZone.localize(df_index_ranges['end_time'].max().to_pydatetime())

if __name__ == "__main__":
    import sys, os
    sys.path.append(os.path.join(os.getcwd(), "qpt_historic_pos", "impl"))

    eod = True
    if eod:
        eod_date = dt.date(2022, 11, 30)
        db_chicago_datetime =  ChicagoTimeZone.localize(dt.datetime.combine(eod_date, dt.time(hour=16, minute=0, second=0)))
        db_utc_time = db_chicago_datetime.astimezone(UtcTimeZone)

    else:
        # Live
        report_date = dt.date.today()
        eod_date = report_date - dt.timedelta(days=1)
        db_chicago_datetime = dt.datetime.now().astimezone(ChicagoTimeZone)
        db_utc_time = db_chicago_datetime.astimezone(UtcTimeZone)

    (
        net_open_positions_df,
        summary_exchange_balances_df,
        summary_asset_loans_cash_df,
        asset_and_open_positions_df
    ) = run(eod_date, db_chicago_datetime,
            trading_repo=qpt_mssql.TradingRepository())

    # Dump to file
    net_open_positions_df.to_csv(f"{db_utc_time:%Y-%m-%d_%H%M%S}_net_open_positions.csv", sep=',')
    summary_exchange_balances_df.to_csv(f"{eod_date:%Y-%m-%d}_summary_exchange_balances_00utc.csv", sep=',')
    asset_and_open_positions_df.to_csv(f"{db_utc_time:%Y-%m-%d_%H%M%S}_asset_and_open_positions.csv", sep=',')
