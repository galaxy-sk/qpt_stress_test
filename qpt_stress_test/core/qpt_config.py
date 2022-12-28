
"""
These lists of accounts are from Bovas NAV code and change regularly
"""

import datetime as dt

# Update from Bovas's generate_daily_nav_00utc.ipynb notebook
datlib_loc = "//10.17.182.172/datlib"
bs_info = {
    "bluescale6" : {
        "account_list" : ["HUBI-3-S3-F", "HUBI-3-S3-M", "HUBI-3-S3-M Loan", "HUBI-3-S3-P", "HUBI-3-S3-E"],
        "proc_name" : "trading.pnl.bluescale6_nav",
        "interest_paid" : 0, #1.917808219 + 4.24657534 + 4.10958904 + 4.24657534 + 4.24657534 + 4.10958904 + 4.24657534 +
            #4.10958904 + 4.24657534 + 4.24657534 + 3.83561644 + 4.24657534 + 4.24657534 + 2.972602740 + 2.8767123387671233 +
            #2.87671233 + 2.97260274 + 2.87671233 + 2.87671233,
        "loans" : [("BTC", 0, 0.10, 365, dt.date(2021,4,16))]
    }
}

exchange_balance_accounts = [
    'SHFT-1-W', 'HUBI-M', 'HUBI-E', 'HUB2-E', 'HUB2-M', 'LMAC', 'LMAX', 'KRKE', 'BTSE', 'PITX-E', 'WOOX-1-M-E',
    'FUB2-M', 'HUBI-3-S3-M','OKEX-2-W1', 'OKEX-2-U1', 'FUBI-M', 'OKEX-2-S1', 'OKEX-2-S2', 'HUBI-1-M-P',
    'FBLK-AUDIT-BINE', 'FBLK-AUDIT-HUBI', 'FBLK-AUDIT-OKEX', 'FBLK-Default', 'FBLK-GOTC-GACM', 'FBLK-LEND-BTGO',
    'FBLK-LEND-DRAW', 'FBLK-LEND-OXTF', 'FBLK-LEND-XRPF', 'FBLK-LMAC-M', 'FBLK-Network Deposits', 'FBLK-PITX-E',
    'FBLK-LEND-CELS', 'FBLK-LEND-GADI', 'FBLK-LEND-NICO', 'FBLK-LEND-GENX','FBLK-BINE-MX-S1','FBLK-DEFI-AAVE',
    'FBLK-GOTC-BIGO','FBLK-DYDX-1-M-P','HUBI-3-S3-F','HUBI-3-S3-E','OKEX-2-S3','BTFX-1-M-E','FTXE-1-M-E',
    'BULL-1-M-E','BULL-1-M-M','DYDX-1-M-P','BINE-MX-S1-P','BINE-MX-S1-F','BINE-MX-S1-E','BINE-MX-S1-M','BINE-2-S1-E',
    'BINE-2-S1-F','BINE-2-S1-M','BINE-2-S1-P','BULL-2-M-E','BULL-2-M-M','HUBI-1-M-PT','BULL-3-M-E','BULL-4-M-E',
    'LEND-GACM','FBLK-WOOX-1-M-E','FBLK-LEND-GACM','DEFI-STRAT-1','GATE-1-M-E','GATE-1-M-M',
    'BINE-2-S2-E','BINE-2-S2-F','BINE-2-S2-M','BINE-2-S2-P','DEFI-STRAT-2','DEFI-STRAT-3','DEFI-STRAT-4',
    'GATE-1-M-C','GATE-1-M-PT','DEFI-STRAT-5','DEFI-STRAT-6','DEFI-STRAT-8'
]

loan_accounts = [
    'LEND-GACM', 'LEND-HUBI', 'LEND-LMAC','LEND-PITX','LEND-XRPF','LEND-OKEX','HUBI-3-S3-M Loan','HUBI-M','HUB2-M',
    'LEND-OXTF','WOOX-1-M-E','FTXE-1-M-E','OKEX-2-U1','LEND-HUBS6','BULL-1-M-M Loan','BULL-2-M-M Loan','LEND-BULL',
    'OKEX-2-S3', 'BINE-2-S1-M', 'DEFI-STRAT-1 Loan', 'GATE-1-M-M Loan', 'BINE-2-S2-M', 'DEFI-STRAT-8 Loan'
]

# Update from Bovas's Excel spreadsheet: in 'NAV Report 00UTC.xls!lookup_table' use: 
#  =""""&A2&""": {"""&A$1&""": """&A2&""", """&B$1&""": """&B2&""", """&C$1&""": """&C2&""", """&D$1&""": """&D2&"""},"
# Add: 
#   "ED&F": {"Account": "ED&F", "BalanceType": "", "TYPE": "COLLATERAL", "Endpoint": "ED&F"},
#   "CASH": {"Account": "CASH", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "CASH"},
account_map = {
    "WEDBUSH": {"Account": "Wedbush", "BalanceType": "", "TYPE": "COLLATERAL", "Endpoint": "WEDBUSH"},
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
    "DEFI-STRAT-8 Loan": {"Account": "DEFI-STRAT-8 Loan", "BalanceType": "", "TYPE": "EXCHLOAN", "Endpoint": "DEFI"},
    "BINE-2-S2-E": {"Account": "BINE-2-S2-E", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "BINE"},
    "BINE-2-S2-F": {"Account": "BINE-2-S2-F", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "BINE"},
    "BINE-2-S2-M": {"Account": "BINE-2-S2-M", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "BINE"},
    "BINE-2-S2-P": {"Account": "BINE-2-S2-P", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "BINE"},
    "BINE-2-S2-M Margin Loan": { 
        "Account": "BINE-2-S2-M Margin Loan", "BalanceType": "", "TYPE": "EXCHLOAN", "Endpoint": "BINE"},
    "DEFI-STRAT-2": {"Account": "DEFI-STRAT-2", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "DEFI"},
    "DEFI-STRAT-3": {"Account": "DEFI-STRAT-3", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "DEFI"},
    "DEFI-STRAT-4": {"Account": "DEFI-STRAT-4", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "DEFI"},
    "DEFI-STRAT-5": {"Account": "DEFI-STRAT-5", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "DEFI"},
    "DEFI-STRAT-6": {"Account": "DEFI-STRAT-6", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "DEFI"},
    "DEFI-STRAT-7": {"Account": "DEFI-STRAT-7", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "DEFI"},
    "DEFI-STRAT-8": {"Account": "DEFI-STRAT-8", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "DEFI"},
    "GATE-1-M-C": {"Account": "GATE-1-M-C", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "GATE"},
    "GATE-1-M-PT": {"Account": "GATE-1-M-PT", "BalanceType": "", "TYPE": "ASSET", "Endpoint": "GATE"},
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
