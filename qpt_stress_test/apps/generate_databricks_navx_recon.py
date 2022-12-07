
import datetime as dt
import pandas as pd
import os
import pymd
from qpt_historic_pos.impl.utils.times import ChicagoTimeZone, UtcTimeZone


def generate_recon(asset_and_open_positions_df: pd.DataFrame, db_chicago_datetime: dt.datetime, trading_repo, marketdata_repo=None):

    # Get marks for all positions:
    pymd.init()
    pymd.enable_logging()
    df_index_ranges = pymd.data_access.coinmetrics_reference_rate_supported_tokens()
    coinmetrics_symbols = list(df_index_ranges['symbol'].unique())
    first_datetime = pymd.UtcTimeZone.localize(df_index_ranges['start_time'].min().to_pydatetime())
    last_datetime = pymd.UtcTimeZone.localize(df_index_ranges['end_time'].max().to_pydatetime())

    get_start_datetime = pymd.UtcTimeZone.localize(db_chicago_datetime - dt.timedelta(days=2))
    get_end_datetime = UtcTimeZone.localize(db_chicago_datetime)
    symbols = set(asset_and_open_positions_df["instrument"])
    underlying = set(asset_and_open_positions_df["underlying"])
    marketdata_repo.reference_rate_ts(symbols, )
    
    return


def load_asset_and_open_positions(chicago_position_dtt: dt.datetime, data_dir: str) -> pd.DataFrame:
    """" Use CSV's rather than tempting fate pulling MSSQL/PGSQL data """
    # Get EOD Datetime as UTC
    utc_datetime = chicago_position_dtt.astimezone(UtcTimeZone)
    csv_filename = f"{utc_datetime:%Y-%m-%d_%H%M%S}_asset_and_open_positions.csv"
    abs_file_path = os.path.abspath(os.path.join(data_dir, csv_filename))
    asset_and_open_positions_df = pd.read_csv(abs_file_path, sep=',', header=0, index_col=0)

    # Remove account ending in ' Margin Loan',
    # Replace account ED&F Man Capital' with 'UNMBF222'
    def map_account_name(acc):
        return acc[:-len(' Margin Loan')] if acc.endswith(' Margin Loan') \
            else 'UNMBF222' if acc ==  "ED&F Man Capital" \
            else acc

    asset_and_open_positions_df["account"] = asset_and_open_positions_df["account"].apply(map_account_name)

    return asset_and_open_positions_df


def run(report_chicago_dtt: dt.datetime):

    # Live
    report_date = report_chicago_dtt.date()
    eod_date = report_date - dt.timedelta(days=1)
    eod_datetime = ChicagoTimeZone.localize(dt.datetime.combine(eod_date, dt.time(hour=16, minute=0, second=0)))

    #
    asset_and_open_positions_df = load_asset_and_open_positions(chicago_position_dtt=eod_datetime, data_dir=".")

    # Group by:
    # ["exchange", "account", "instrument_type", "underlying", "instrument"

    # The same swap/future in different accounts can come back with different market prices
    summary_mark_df = asset_and_open_positions_df[["instrument", "mark_price"]] \
        .groupby(["instrument"]) \
        .mean() \
        .reset_index()
    summary_notional_df = asset_and_open_positions_df[["exchange", "account", "instrument", "notional"]] \
        .groupby(["exchange", "account", "instrument"]) \
        .sum() \
        .reset_index()
    summary_position_df = asset_and_open_positions_df[["exchange", "account", "instrument", "position"]] \
        .groupby(["exchange", "account", "instrument"]) \
        .sum() \
        .reset_index()
    summary_unrealized_df = asset_and_open_positions_df[["instrument", "unrealized_pnl"]] \
        .groupby(["instrument"]) \
        .sum() \
        .reset_index()


    return

if __name__ == "__main__":

    run(report_chicago_dtt=dt.datetime.now().astimezone(ChicagoTimeZone))
    exit()

