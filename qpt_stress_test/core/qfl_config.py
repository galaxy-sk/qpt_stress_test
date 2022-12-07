
"""
These lists of accounts are from Bovas NAV code and change regularly
"""

import datetime as dt

datlib_loc = "//10.17.182.172/datlib"
bs_info = {
    "bluescale6": {
        "account_list": ["HUBI-3-S3-F", "HUBI-3-S3-M", "HUBI-3-S3-M Loan", "HUBI-3-S3-P", "HUBI-3-S3-E"],
        "proc_name": "trading.pnl.bluescale6_nav",
        "interest_paid": 1.917808219 + 4.24657534 + 4.10958904 + 4.24657534 + 4.24657534 + 4.10958904 + 4.24657534 +
                         4.10958904 + 4.24657534 + 4.24657534 + 3.83561644 + 4.24657534 + 4.24657534 + 2.972602740 + 2.8767123387671233 +
                         2.87671233 + 2.97260274 + 2.87671233 + 2.87671233,
        "loans": [("BTC", 500, 0.10, 365, dt.date(2021, 4, 16))]
    }
}

exchanges_balance_accounts = [
    'SHFT-1-W', 'HUBI-M', 'HUBI-E', 'HUB2-E', 'HUB2-M', 'LMAC', 'LMAX', 'KRKE', 'BTSE', 'PITX-E', 'WOOX-1-M-E',
    'FUB2-M', 'HUBI-3-S3-M', 'OKEX-2-W1', 'OKEX-2-U1', 'FUBI-M', 'OKEX-2-S1', 'OKEX-2-S2', 'HUBI-1-M-P',
    'FBLK-AUDIT-BINE', 'FBLK-AUDIT-HUBI', 'FBLK-AUDIT-OKEX', 'FBLK-Default', 'FBLK-GOTC-GACM', 'FBLK-LEND-BTGO',
    'FBLK-LEND-DRAW', 'FBLK-LEND-OXTF', 'FBLK-LEND-XRPF', 'FBLK-LMAC-M', 'FBLK-Network Deposits', 'FBLK-PITX-E',
    'FBLK-LEND-CELS', 'FBLK-LEND-GADI', 'FBLK-LEND-NICO', 'FBLK-LEND-GENX', 'FBLK-BINE-MX-S1', 'FBLK-DEFI-AAVE',
    'FBLK-GOTC-BIGO', 'FBLK-DYDX-1-M-P', 'HUBI-3-S3-F', 'HUBI-3-S3-E', 'OKEX-2-S3', 'BTFX-1-M-E', 'FTXE-1-M-E',
    'BULL-1-M-E', 'BULL-1-M-M', 'DYDX-1-M-P', 'BINE-MX-S1-P', 'BINE-MX-S1-F', 'BINE-MX-S1-E', 'BINE-MX-S1-M',
    'BINE-2-S1-E',
    'BINE-2-S1-F', 'BINE-2-S1-M', 'BINE-2-S1-P', 'BULL-2-M-E', 'BULL-2-M-M', 'HUBI-1-M-PT', 'BULL-3-M-E', 'BULL-4-M-E',
    'LEND-GACM', 'FBLK-WOOX-1-M-E', 'FBLK-LEND-GACM', 'DEFI-STRAT-1', 'GATE-1-M-E', 'GATE-1-M-M',
    'BINE-2-S2-E', 'BINE-2-S2-F', 'BINE-2-S2-M', 'BINE-2-S2-P', 'DEFI-STRAT-2', 'DEFI-STRAT-3', 'DEFI-STRAT-4']

loans_accounts = [
    'LEND-GACM', 'LEND-HUBI', 'LEND-LMAC', 'LEND-PITX', 'LEND-XRPF', 'LEND-OKEX', 'HUBI-3-S3-M Loan', 'HUBI-M',
    'HUB2-M',
    'LEND-OXTF', 'WOOX-1-M-E', 'FTXE-1-M-E', 'OKEX-2-U1', 'LEND-HUBS6', 'BULL-1-M-M Loan', 'BULL-2-M-M Loan',
    'LEND-BULL',
    'OKEX-2-S3', 'BINE-2-S1-M', 'DEFI-STRAT-1 Loan', 'GATE-1-M-M Loan', 'BINE-2-S2-M']
