
import os
import sys
from datetime import datetime, timedelta
import pandas as pd
import copy
import base64
import clickhouse_driver

from .utils.times import ChicagoTimeZone, UtcTimeZone
from .utils import times, pgutils, dbutils


db_pg = pgutils.PgTemplate(db_host='bfc-rds-2.cq7gi4betiom.us-east-2.rds.amazonaws.com',
                           db_user='hubble',
                           db_password='Telescope',
                           db_name='trading',
                           maxconn=1)

db_pg.connect()

db_ms = dbutils.DBTemplate(db_host='sv-awoh-dw01.na.bluefirecap.net',
                                      db_user='ro',
                                       db_password='ro',
                                       db_name='RawData')

def fmt_ms_timestamp(dt):
    return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


def parse_hstore(s):
    ls = {}
    if not s:
        return ls
    pairs = s.split(',')
    for p in pairs:
        kv = p.split('=>')
        k = kv[0].strip().strip('"')
        v = float(kv[1].strip().strip('"'))
        ls[k] = v
    return ls


def get_position_snapshot_at(db_pg, dt):
    min_time = dt - timedelta(minutes=5)
    max_time = dt + timedelta(minutes=10)

    def _get_snapshot(conn):
        sql = f"""
    with bar as (
    select account, min(snapshot_time) as min_time
    from rms.exchange_position_snapshot
    where snapshot_time between '{fmt_ms_timestamp(min_time)}' and '{fmt_ms_timestamp(max_time)}'
    group by account
    )
    select t1.exchange, t1.account, t1.snapshot_time, 
    t1.long_positions_notional, t1.short_positions_notional, t1.long_positions, t1.short_positions,
    t1.mark_prices, t1.long_unrealized_pnls, t1.short_unrealized_pnls
    from rms.exchange_position_snapshot t1
    inner join bar t2 on t1.account = t2.account and t1.snapshot_time = t2.min_time
        """
        print(sql)
        return pd.read_sql_query(sql, conn)

    df_snap = db_pg.transaction_callback(_get_snapshot)
    return df_snap


def get_cme_position_as_dict(db_ms, dt_utc):
    dt_chi = dt_utc.astimezone(ChicagoTimeZone)
    ret = db_ms.query(f"""
select symbol, sum(case when side = 'S' then - LastFillQuantity else LastFillQuantity end)
from Trading.dbo.fills with(nolock)
 where Account = 'UNMBF222'
 and date <= '{fmt_ms_timestamp(dt_chi)}'
group by symbol
having abs(sum(case when side = 'S' then - LastFillQuantity else LastFillQuantity end)) != 0
union
select symbol, sum(case when side = 'S' then - LastFillQuantity else LastFillQuantity end)
from Trading.dbo.backfill with(nolock)
 where Account = 'UNMBF222'
 and date <= '{fmt_ms_timestamp(dt_chi)}'
group by symbol
having abs(sum(case when side = 'S' then - LastFillQuantity else LastFillQuantity end)) != 0
    """)
    d = {}
    for row in ret:
        sym = row[0].upper()
        pos = int(row[1])
        if sym not in d:
            d[sym] = 0
        d[sym] = d[sym] + pos
    return d


def get_cme_eod_settlement_px(db_ms, dt_chi):
    ret = db_ms.query(f"""
    with bar as (
select symbol, min(asof) as mt
from RawData.cme.crypto_settlement 
where asof >= '{dt_chi.strftime('%Y-%m-%d')} 16:00:00'
and asof <= '{dt_chi.strftime('%Y-%m-%d')} 23:00:00'
group by symbol )
select t1.symbol, t1.price_settlement 
from RawData.cme.crypto_settlement t1
inner join bar on bar.symbol = t1.symbol and bar.mt = t1.asof 
""")
    return {str(row[0]).upper(): float(row[1]) for row in ret}


def get_cme_px_as_dict(db_ms, dt_utc):
    dt_chi = dt_utc.astimezone(ChicagoTimeZone)
    if (dt_chi.hour == 16 and dt_chi.minute < 5) or (dt_chi.hour == 15 and dt_chi.minue >= 55):
        px = get_cme_eod_settlement_px(db_ms, dt_chi)
        if px:
            return px
    min_time = dt_utc - timedelta(minutes=65)
    max_time = dt_utc + timedelta(minutes=10)
    ck = clickhouse_driver.Client(
        host='sv-awoh-md1.na.bluefirecap.net'
        , user='reader1'
        , compression=False
        , send_receive_timeout=600  # query must return in 10 minutes, max RAM is 10GB
        , settings={'use_numpy': False
            , 'strings_encoding': 'ascii'}
    )
    ret = ck.execute(f"""
with bar as (
 select name, max(`timestamp`) as mt
 from mark_prices.CME_future_1second 
where `timestamp` BETWEEN '{min_time.strftime('%Y-%m-%d %H:%M:%S')}' and '{max_time.strftime('%Y-%m-%d %H:%M:%S')}'
 group by name
 )
 select name, price
from mark_prices.CME_future_1second t1
inner join bar on bar.name = t1.name and bar.mt = t1.`timestamp` 
order by t1.name asc
    """, types_check=False, columnar=False)
    return {str(row[0]).upper(): float(row[1]) for row in ret}


def get_ftx_mark_prices(db_pg, dt):
    min_time = dt - timedelta(minutes=6)
    max_time = dt + timedelta(minutes=2)
    _sql = f"""
    with bar as (
  select name, max(timestamp) MAX_TIME
  FROM [RawData].[ftxe].[future_mark_price] with(nolock)
  where [timestamp] between '{fmt_ms_timestamp(min_time)}' and '{fmt_ms_timestamp(max_time)}'
  group by name)
  select   b3.symbol_bfc, B1.[mark]
  FROM [RawData].[ftxe].[future_mark_price]  B1 with(nolock)
  inner join bar as B2 with(nolock) on B1.name = B2.name and B1.[timestamp] = B2.MAX_TIME
  left join Trading.crypto.instrument_reference b3 on b3.exchange = 'FTX' and b3.instrument_type in ('SWAP', 'FUTURE')
    and b3.symbol_exch = b1.name
    """
    #     print(_sql)
    _ret = db_pg.query(_sql)
    return {r[0]: float(r[1]) for r in _ret}


def get_active_contracts(db_pg, dt):
    def q(conn):
        return pd.read_sql(f"""
 	select upper(exchange) as exchange, symbol_bfc as instrument, instrument_type, expiration_time, is_linear,  case when is_linear > 0 then currency_position else currency_settlement end as underlying
    from Trading.definition.instrument_reference
	where symbol_bfc is not null
    and instrument_type in ('FUTURE', 'SWAP')
    and expiration_time >= '{fmt_ms_timestamp(dt)}'
    limit 5000
    """, conn)

    return db_pg.transaction_callback(q)


def gen_open_position_report(dt_target):
    global db_pg
    # global db_ms, db_pg
    # mark_price_ftx = get_ftx_mark_prices(mssql_db, dt_target)
    df_inst = get_active_contracts(db_pg, dt_target)
    psnap = get_position_snapshot_at(db_pg, dt_target)

    dat_exch_pos = psnap.to_dict('records')
    # acc -> inst -> net pos[pos, notional, mark]
    inst_pos_net = {}
    # acc -> inst -> unrealized pnl
    inst_pos_u_pnl = {}

    acc2exch = {}
    for acc_pos in dat_exch_pos:
        long_pos = parse_hstore(acc_pos['long_positions'])
        short_pos = parse_hstore(acc_pos['short_positions'])
        long_pos_not = parse_hstore(acc_pos['long_positions_notional'])
        short_pos_not = parse_hstore(acc_pos['short_positions_notional'])
        mark_prices = parse_hstore(acc_pos['mark_prices'])
        short_unrealized_pnls = parse_hstore(acc_pos['short_unrealized_pnls'])
        long_unrealized_pnls = parse_hstore(acc_pos['long_unrealized_pnls'])

        for sym, pos in long_pos.items():
            acc = acc_pos['account'].upper()
            exch = acc_pos['exchange'].upper()
            acc2exch[acc] = exch
            if acc not in inst_pos_net:
                inst_pos_net[acc] = {}
            if acc not in inst_pos_u_pnl:
                inst_pos_u_pnl[acc] = {}

            sym_mark_px = mark_prices[sym] if sym in mark_prices else None

            last_pos = 0
            last_notional = 0
            if sym in inst_pos_net[acc]:
                last_pos = inst_pos_net[acc][sym][0]
                last_notional = inst_pos_net[acc][sym][1]
            last_pos += abs(pos)

            sym_long_pos_not = None
            if sym in long_pos_not:
                sym_long_pos_not = long_pos_not[sym]

            last_notional += abs(sym_long_pos_not)
            inst_pos_net[acc][sym] = [last_pos, last_notional, sym_mark_px]

            if sym in long_unrealized_pnls:
                inst_pos_u_pnl[acc][sym] = long_unrealized_pnls[sym]

        for sym, pos in short_pos.items():
            acc = acc_pos['account'].upper()
            exch = acc_pos['exchange'].upper()
            acc2exch[acc] = exch
            if acc not in inst_pos_net:
                inst_pos_net[acc] = {}
            if acc not in inst_pos_u_pnl:
                inst_pos_u_pnl[acc] = {}
            sym_mark_px = mark_prices[sym] if sym in mark_prices else None
            last_pos = 0
            last_notional = 0
            if sym in inst_pos_net[acc]:
                last_pos = inst_pos_net[acc][sym][0]
                last_notional = inst_pos_net[acc][sym][1]
            last_pos -= abs(pos)
            sym_short_pos_not = None
            if sym in short_pos_not:
                sym_short_pos_not = short_pos_not[sym]
            if sym_short_pos_not is not None:
                last_notional -= abs(sym_short_pos_not)
            else:
                last_notional -= 0
            inst_pos_net[acc][sym] = [last_pos, last_notional, sym_mark_px]

            if sym in short_unrealized_pnls:
                upnl = short_unrealized_pnls[sym]
                last_u_pnl = 0
                if sym in inst_pos_u_pnl[acc]:
                    last_u_pnl = inst_pos_u_pnl[acc][sym]
                inst_pos_u_pnl[acc][sym] = last_u_pnl + upnl

    inst_pos = []
    for acc, sym_pos in inst_pos_net.items():
        for sym, pos3 in sym_pos.items():
            u_pnl = 0
            sym_mark = None
            if acc in inst_pos_u_pnl and sym in inst_pos_u_pnl[acc]:
                u_pnl = inst_pos_u_pnl[acc][sym]
            if abs(pos3[0]) >= 1e-6:
                inst_pos.append({
                    'exchange': acc2exch[acc],
                    'account': acc,
                    'instrument': sym,
                    'position': pos3[0],
                    'notional': pos3[1],
                    'mark_price': pos3[2],
                    'unrealized_pnl': u_pnl
                })
            # else:
            #     print('skip', acc, sym, pos2[0], pos2[1])

    ##cme_pos = get_cme_position_as_dict(db_ms, dt_target)
    #cme_mark_px = get_cme_px_as_dict(db_ms, dt_target)
    """"or sym, pos in cme_pos.items():
        mark_px = None
        notional = None
        if sym in cme_mark_px:
            mark_px = cme_mark_px[sym]
            notional = pos * mark_px
        inst_pos.append({
                    'exchange': 'CME',
                    'account': 'UNMBF222',
                    'instrument': sym,
                    'position': pos,
                    'notional': notional,
                    'mark_price': mark_px,
                    'unrealized_pnl': None
                })"""
    df_inst_pos = pd.DataFrame.from_dict(inst_pos, orient='columns')

    df_m = pd.merge(df_inst_pos, df_inst, on=['exchange', 'instrument'], how='left')
    return df_m



