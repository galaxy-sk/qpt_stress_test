
import platform
import datetime
import re
from typing import Dict, Set
import configparser
from collections import namedtuple
from abc import ABCMeta, abstractmethod

import pyodbc
from . import times


class DBTemplate(object):

    def __init__(self, **kwargs):
        self.db_host = kwargs['db_host']
        self.db_user = kwargs['db_user']
        self.db_password = kwargs['db_password']
        self.db_name = kwargs['db_name']
        if 'timeout' in kwargs:
            self.timeout = int(kwargs['timeout'])
        else:
            self.timeout = 300
        self.odbc_name = self._find_driver_name()
        if not self.odbc_name:
            raise RuntimeError('cannot find driver')

    def _find_driver_name(self):
        osnames = platform.platform().lower()
        self.odbc_name = None
        if 'win' in osnames:
            return 'SQL Server Native Client 11.0'

        cp = configparser.ConfigParser()
        cp.read('/etc/odbcinst.ini')
        driver_disc = set()
        candidate_ls = ['ODBC Driver 17 for SQL Server', 'FreeTDS']
        for name, cfg in cp.items():
            for t in candidate_ls:
                if t == str(name):
                    driver_disc.add(t)
                    break

        driver_name = None
        for c in candidate_ls:
            if c in driver_disc:
                driver_name = c
                break

        return driver_name

    def _get_connection(self):
        pyodbc.pooling = True
        driver_addr = 'DRIVER={};SERVER={};PORT=1433;UID={};PWD={};DATABASE={}'.format(self.odbc_name,
            self.db_host, self.db_user, self.db_password, self.db_name)
        # pyodbc locks the schema when `autocommit=False`
        # conn = pyodbc.connect(driver_addr, autocommit=False)
        conn = pyodbc.connect(driver_addr, autocommit=True)
        return conn

    def transaction_callback(self, func):
        ret = None
        with self._get_connection() as conn:
            try:
                ret = func(conn)
                conn.commit()
            except:
                conn.rollback()
                raise
        return ret

    def cursor_callback(self, func):
        def with_connection(conn):
            cursor = conn.cursor()
            ret = func(cursor)
            cursor.close()
            return ret

        return self.transaction_callback(with_connection)

    def exec_sql(self, sql: str):
        def with_cursor(cur):
            cur.execute(sql)
            return cur.rowcount

        return self.cursor_callback(with_cursor)

    def query(self, sql: str):
        def with_cursor(cur):
            cur.execute(sql)
            return cur.fetchall()

        return self.cursor_callback(with_cursor)


def fmt_sql_timestamp(dt: datetime.datetime, timezone=times.ChicagoTimeZone):
    if not dt.tzinfo:
        dt = timezone.localize(dt)
    elif dt.tzinfo != timezone:
        dt = dt.astimezone(timezone)
    return dt.strftime('%Y-%m-%d %H:%M:%S.%f')


def fmt_mssql_datetime(dt: datetime.datetime, timezone=times.ChicagoTimeZone):
    return fmt_sql_timestamp(dt, timezone)[:-3]


def fmt_sql_date(dt, timezone=times.ChicagoTimeZone):
    if isinstance(dt, datetime.date):
        return dt.strftime('%Y-%m-%d')
    if not dt.tzinfo:
        dt = timezone.localize(dt)
    elif dt.tzinfo != timezone:
        dt = dt.astimezone(timezone)
    return dt.strftime('%Y-%m-%d')


def fmt_date_yyyymmdd(dt, timezone=times.ChicagoTimeZone):
    if isinstance(dt, datetime.date):
        return dt.strftime('%Y%m%d')
    if not dt.tzinfo:
        dt = timezone.localize(dt)
    elif dt.tzinfo != timezone:
        dt = dt.astimezone(timezone)
    return dt.strftime('%Y%m%d')


def fmt_sql_timestamp_tz(dt: datetime, timezone=times.ChicagoTimeZone):
    if not dt.tzinfo:
        dt = timezone.localize(dt)
    elif dt.tzinfo != timezone:
        dt = dt.astimezone(timezone)
    return dt.strftime('%Y-%m-%d %H:%M:%S%z')[:-2]


###############################################################################
# A simplest ORM implementation for Python Objects

def flatten_object(obj):
    dgt = obj
    if type(obj) != dict:
        if hasattr(obj, '_asdict'):
            dgt = getattr(obj, '_asdict')()
        elif hasattr(obj, '__dict__'):
            dgt = vars(obj)
    kv = {}
    for k in dgt.keys():
        lk = k.lower()
        if lk not in kv:
            kv[lk] = dgt[k]
        else:
            raise RuntimeError('duplicated key {} in case-insensitive context'.format(lk))
    return kv


def flatten_db_rows(row_ls, name_overrides={}):
    r0 = row_ls[0]
    key_names = []
    for f in r0.cursor_description:
        key_names.append(f[0])

    rows_data = []
    for row in row_ls:
        row_data = {}
        for i in range(0, len(row)):
            db_name = key_names[i]
            obj_name = db_name
            if db_name in name_overrides:
                obj_name = name_overrides[db_name]
            row_data[obj_name] = row[i]
        rows_data.append(row_data)
    return rows_data


ColumnMeta = namedtuple('ColumnMeta', 'name dtype default_val nullable max_len num_precision')
ColumnConverter = namedtuple('ColumnConverter', 'name convert')
NameColConverter = namedtuple('NameColConverter', 'obj_name db_name convert')

_default_col_converter = ColumnConverter('*', lambda o: str(o) if o is not None else 'NULL')


class TableMeta:
    int_dtypes = {'bigint', 'numeric', 'smallint', 'decimal', 'int', 'tinyint', 'bit', 'integer', 'boolean'}
    float_dtypes = {'float', 'real', 'double precision', 'double'}
    str_types = {'char', 'varchar', 'text', 'character varying', 'cidr', 'inet', 'macaddr', 'macaddr8'}

    int_limits = {'tinyint': (0, 255),
                  'smallint': (-32767, 32768),
                  'int': (-2147483648, 2147483647),
                  'bigint': (-9223372036854775808, 9223372036854775807)}

    def __init__(self, table_catalog: str, table_schema: str, table_name: str, canonical_name: str):
        self.table_catalog = table_catalog
        self.table_schema = table_schema
        self.table_name = table_name
        self.canonical_name = canonical_name
        self.nullable_cols: Dict[str, ColumnMeta] = {}
        self.nonnull_cols: Dict[str, ColumnMeta] = {}
        self.primary_keys: Set[str] = {}
        self.converters = {}

    @staticmethod
    def should_quote(col_meta: ColumnMeta, str_val: str):
        return (col_meta.dtype in TableMeta.str_types
                or str(col_meta.dtype).startswith('datetime')
                or str(col_meta.dtype).startswith('timestamp')
                or col_meta.dtype == 'USER-DEFINED'
                ) and str_val.lower() != 'null'


class BaseObjRelationMapping(metaclass=ABCMeta):

    def __init__(self, db_access: DBTemplate):
        self.db_access: DBTemplate = db_access
        self.registered_tables: Dict[str, TableMeta] = {}
        self.default_converters = {}

    @abstractmethod
    def parse_canonical_name(self, tname: str):
        pass

    @abstractmethod
    def canonical_name(self, table_catalog: str, table_schema: str, table_name: str):
        pass

    def register_schema(self, catelog: str, schema: str):
        tabs = self.db_access.query("""
    select TABLE_NAME
    from INFORMATION_SCHEMA.TABLES
    where TABLE_CATALOG = '{}' 
    and TABLE_SCHEMA = '{}'""".format(catelog, schema))
        self.register_tables([self.canonical_name(catelog, schema, t[0]) for t in tabs])

    def register_tables(self, tls):
        for table in tls:
            self.register_table(*self.parse_canonical_name(table))

    def register_table(self, table_catalog: str, table_schema: str, table_name: str, additional_converters=None):
        canonical_name = self.canonical_name(table_catalog, table_schema, table_name).lower()
        meta_info = TableMeta(table_catalog, table_schema, table_name, canonical_name)

        nullable_cols = {}
        nonnull_cols = {}
        col_info = self.db_access.query("""
select COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_PRECISION_RADIX
from INFORMATION_SCHEMA.COLUMNS
where TABLE_CATALOG = '{}' 
        and TABLE_SCHEMA = '{}'
        and TABLE_NAME = '{}'""".format(table_catalog, table_schema, table_name))
        for col in col_info:
            name = col[0].lower()
            dv = col[3]
            num_p = col[5]
            max_len = col[4]
            data_type = str(col[1]).lower().strip()
            if max_len is None and data_type in TableMeta.str_types:
                max_len = 99999
            ci = ColumnMeta(name=name, dtype=data_type, default_val=dv,
                            nullable=True if col[2].upper() == 'YES' else False,
                            max_len=max_len, num_precision=num_p)
            if ci.nullable:
                nullable_cols[ci.name] = ci
            else:
                nonnull_cols[ci.name] = ci
        meta_info.nonnull_cols = nonnull_cols
        meta_info.nullable_cols = nullable_cols

        pk_info = self.db_access.query("""
        select Col.Column_Name from 
            INFORMATION_SCHEMA.TABLE_CONSTRAINTS as Tab, 
            INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE as Col 
        where
            Col.Constraint_Name = Tab.Constraint_Name
            and Col.Table_Name = Tab.Table_Name
            and Constraint_Type = 'PRIMARY KEY'
            and Tab.TABLE_CATALOG = '{}'
            and Tab.TABLE_SCHEMA = '{}'
            and Tab.TABLE_NAME = '{}'
        """.format(table_catalog, table_schema, table_name))
        meta_info.primary_keys = set([r[0].lower() for r in pk_info])

        if additional_converters:
            meta_info.converters.update(additional_converters)

        self.registered_tables[canonical_name] = meta_info
        return meta_info

    def primary_keys(self, table_catalog: str, table_schema: str, table_name: str):
        canonical_name = self.canonical_name(table_catalog, table_schema, table_name)
        if canonical_name not in self.registered_tables:
            err_msg = '{} not registered yet'.format(canonical_name)
            raise RuntimeError(err_msg)
        tab_meta = self.registered_tables[canonical_name]
        return tab_meta.primary_keys

    def compose_multi_deletes(self, canonical_name, keys, objs, override_vals=None):
        c, s, t = self.parse_canonical_name(canonical_name)
        return [self.compose_delete(obj, c, s, t, keys, override_vals) for obj in objs]

    def _where_clause(self, table_catalog, table_schema, table_name, obj, keys, override_vals):
        canonical_name = self.canonical_name(table_catalog, table_schema, table_name)
        if canonical_name not in self.registered_tables:
            err_msg = '{} not registered yet'.format(canonical_name)
            raise RuntimeError(err_msg)
        tab_meta = self.registered_tables[canonical_name]
        if not keys:
            keys = tab_meta.primary_keys
        keys = [k.lower() for k in keys]

        fobj = self._normalize_obj(canonical_name, obj, override_vals)
        conds = []
        for k in keys:
            col_meta = tab_meta.nullable_cols[k] if k in tab_meta.nullable_cols else tab_meta.nonnull_cols[k]
            if TableMeta.should_quote(col_meta, fobj[k]):
                conds.append("{} = '{}'".format(k, fobj[k]))
            else:  # warning: integer types and other types, e.g, datetime is not quoted
                conds.append('{} = {}'.format(k, fobj[k]))
        return fobj, tab_meta, ' and '.join(conds)

    def compose_delete(self, table_catalog, table_schema, table_name, obj, keys=None, override_vals=None):
        fobj, tab_meta, where = self._where_clause(table_catalog, table_schema, table_name, obj, keys, override_vals)
        return "delete from {} where {} ".format(tab_meta.canonical_name, where)

    def count(self, table_catalog, table_schema, table_name, obj, keys=None, override_vals=None):
        fobj, tab_meta, where = self._where_clause(table_catalog, table_schema, table_name, obj, keys, override_vals)
        ret = self.db_access.query("""select count(1) from {} where {} """.format(tab_meta.canonical_name, where))
        if ret and ret[0]:
            return ret[0][0]
        else:
            return 0

    def compose_update(self, table_catalog, table_schema, table_name, obj, keys=None, override_vals=None):
        fobj, tab_meta, where = self._where_clause(table_catalog, table_schema, table_name, obj, keys, override_vals)
        parts = []
        for name, char_arr in fobj.items():
            col_meta = tab_meta.nullable_cols[name] if name in tab_meta.nullable_cols else tab_meta.nonnull_cols[name]
            part = '{}='.format(col_meta.name)
            if TableMeta.should_quote(col_meta, char_arr):
                part += "'{}'".format(char_arr)
            else:
                part += '{}'.format(char_arr)
            parts.append(part)
        return """update {} set {} where {} """.format(tab_meta.canonical_name, ', '.join(parts), where)

    def compose_batch_insert_unique(self, table_catalog, table_schema, table_name, obj_ls, keys=None, override_vals=None):
        new_objs = [obj for obj in obj_ls if
                    self.count(table_catalog, table_schema, table_name, obj, keys, override_vals) == 0]
        if new_objs:
            return self.compose_batch_insert(table_catalog, table_schema, table_name, new_objs, override_vals)
        else:
            return None

    # build one single sql statement that inserts all objects
    def compose_batch_insert(self, table_catalog, table_schema, table_name, obj_ls, override_vals=None):
        canonical_name = self.canonical_name(table_catalog, table_schema, table_name)
        ls_flated = [self._normalize_obj(canonical_name, obj, override_vals) for obj in obj_ls]
        ordered_names = list(set.union(*[set(d.keys()) for d in ls_flated]))
        sql_insert = 'insert into {}({})values'.format(canonical_name, ','.join(ordered_names))

        tab_meta = self.registered_tables[canonical_name]
        sql_values = ''
        for obj in ls_flated:
            sql_values += '('
            for col_name in ordered_names:
                col_meta = tab_meta.nullable_cols[col_name] if col_name in tab_meta.nullable_cols else \
                tab_meta.nonnull_cols[col_name]
                if col_name in obj:
                    if TableMeta.should_quote(col_meta, obj[col_name]):
                        sql_values += "'{}',".format(obj[col_name].replace("'", "''"))
                    else:  # warning: integer types and other types, e.g, datetime is not quoted
                        sql_values += '{},'.format(obj[col_name])
                else:
                    sql_values += 'NULL,'
            sql_values = sql_values[:len(sql_values) - 1]
            sql_values += '),'
        sql_values = sql_values[:len(sql_values) - 1]
        return sql_insert + sql_values

    def compose_insert_unique(self, table_catalog, table_schema, table_name, obj, keys, override_vals=None):
        if self.count(table_catalog, table_schema, table_name, obj, keys, override_vals) == 0:
            return self.compose_insert(table_catalog, table_schema, table_name, obj, override_vals)
        else:
            return None

    def compose_insert(self, table_catalog, table_schema, table_name, obj, override_vals=None):
        canonical_name = self.canonical_name(table_catalog, table_schema, table_name)
        fobj = self._normalize_obj(canonical_name, obj, override_vals)
        tab_meta = self.registered_tables[canonical_name]
        part_names = ''
        part_vals = ''
        for name, char_arr in fobj.items():
            col_meta = tab_meta.nullable_cols[name] if name in tab_meta.nullable_cols else tab_meta.nonnull_cols[name]
            part_names += '{},'.format(col_meta.name)
            if TableMeta.should_quote(col_meta, char_arr):
                part_vals += "'{}',".format(char_arr)
            else:
                part_vals += '{},'.format(char_arr)
        part_names = part_names[:len(part_names) - 1]
        part_vals = part_vals[:len(part_vals) - 1]
        return """insert into {}({})values({})""".format(canonical_name, part_names, part_vals)

    # to skip a field, set the field to None in `override_vals`
    # e.g., override_vals = {'field_to_skip':None}
    def _normalize_obj(self, canonical_name, obj, override_vals=None):
        if canonical_name not in self.registered_tables:
            raise RuntimeError('table {} not registered or name is not canonical'.format(canonical_name))
        dgt = flatten_object(obj)
        assert (not override_vals or isinstance(override_vals, dict))
        flat_chars = {}
        if override_vals:
            for k, v in override_vals.items():
                flat_chars[k.lower()] = v
        override_vals = flat_chars
        tab_meta = self.registered_tables[canonical_name]
        flat_data = {}
        cols = list(tab_meta.nonnull_cols.values())
        cols.extend(list(tab_meta.nullable_cols.values()))
        for col_meta in cols:
            if override_vals and col_meta.name in override_vals:
                if override_vals[col_meta.name] is not None:
                    flat_data[col_meta.name] = str(override_vals[col_meta.name])
            #    if is None, skip that val
            else:
                cnv = None
                if col_meta.name in tab_meta.converters:
                    cnv = tab_meta.converters[col_meta.name]
                elif col_meta.name in self.default_converters:
                    cnv = self.default_converters[col_meta.name]
                else:
                    cnv = _default_col_converter
                obj_name = col_meta.name
                if isinstance(cnv, NameColConverter):
                    obj_name = cnv.obj_name

                if obj_name in dgt:
                    str_val = cnv.convert(dgt[obj_name])
                    if col_meta.dtype in TableMeta.str_types:
                        if 0 < col_meta.max_len < len(str_val):
                            err_msg = 'string [{}] too long for [{}] in table [{}]'.format(str_val, col_meta.name,
                                                                                       canonical_name)
                            raise RuntimeError(err_msg)
                        if len(str_val) < 1:
                            flat_data[col_meta.name] = 'NULL'
                    elif col_meta.dtype in TableMeta.int_dtypes and col_meta.dtype in TableMeta.int_limits:
                        iv = None
                        try:
                            iv = int(float(str_val))
                        except:
                            if not col_meta.nullable:
                                err_msg = 'cannot parse non-null integer [{}] in table[{}], column[{}]'.format(
                                    str_val, canonical_name, col_meta.name)
                                raise RuntimeError(err_msg)
                        if iv:
                            il = TableMeta.int_limits[col_meta.dtype]
                            if iv < il[0] or iv > il[1]:
                                err_msg = 'integer [{}] overflows [{}] (type:{}) in table[{}]'.format(
                                    str_val, col_meta.name, col_meta.dtype, canonical_name)
                                raise RuntimeError(err_msg)
                    flat_data[col_meta.name] = str_val
                elif col_meta.default_val:
                    # print('using database default value for non-null field {}'.format(col_meta.name))
                    flat_data[col_meta.name] = cnv.convert(col_meta.default_val)
                elif not col_meta.nullable:
                    err_msg = 'cannot find value for nonnull field [{}] in [{}]'.format(col_meta.name, tab_meta.table_name)
                    raise RuntimeError(err_msg)
        return flat_data


class SimpleObjRelationMapping(BaseObjRelationMapping):

    def __init__(self, **kwargs):
        db_access = DBTemplate(db_host=kwargs['db_host'],
                                    db_user=kwargs['db_user'],
                                    db_password=kwargs['db_password'],
                                    db_name=kwargs['db_name'])
        super(SimpleObjRelationMapping, self).__init__(db_access)

    def parse_canonical_name(self, tname):
        tname = re.sub('[\s+\[\]]', '', tname)
        t3 = tname.split('.')
        assert (len(t3) == 3)
        return t3[0], t3[1], t3[2]

    def canonical_name(self, table_catalog, table_schema, table_name):
        return '[{}].[{}].[{}]'.format(table_catalog, table_schema, table_name).lower()
