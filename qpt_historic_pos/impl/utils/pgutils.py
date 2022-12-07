
from psycopg2 import pool
from . import dbutils
import re


class PgTemplate(object):

    def __init__(self, **kwargs):
        self.db_host = kwargs['db_host']
        self.db_user = kwargs['db_user']
        self.db_password = kwargs['db_password']
        self.db_name = kwargs['db_name']
        self.minconn = kwargs['minconn'] if 'minconn' in kwargs else 0
        self.maxconn = kwargs['maxconn'] if 'maxconn' in kwargs else 3
        self.pool = None

    def connect(self, pool_klass=pool.SimpleConnectionPool):
        if self.pool is None:
            self.pool = pool_klass(minconn=self.minconn,
                                   maxconn=self.maxconn,
                                   host=self.db_host,
                                   user=self.db_user,
                                   password=self.db_password,
                                   database=self.db_name)
        else:
            self.stop()
            self.connect(pool_klass)

    def stop(self):
        if self.pool:
            self.pool.closeall()
            self.pool = None

    def is_connected(self):
        return self.pool is not None and not self.pool.closed

    def transaction_callback(self, func):
        ret = None
        conn = self.pool.getconn()
        try:
            ret = func(conn)
            conn.commit()
        except:
            conn.rollback()
            raise
        finally:
            self.pool.putconn(conn)
        return ret

    def cursor_callback(self, func):
        def with_connection(conn):
            cursor = conn.cursor()
            ret = func(cursor)
            cursor.close()
            return ret

        return self.transaction_callback(with_connection)

    def exec_sql(self, sql):
        def with_cursor(cur):
            cur.execute(sql)
            return cur.rowcount

        return self.cursor_callback(with_cursor)

    def query(self, sql):
        def with_cursor(cur):
            cur.execute(sql)
            return cur.fetchall()

        return self.cursor_callback(with_cursor)


class PgORM(dbutils.BaseObjRelationMapping):

    def __init__(self, **kwargs):
        max_conn = kwargs['maxconn'] if 'maxconn' in kwargs else 3
        min_conn = kwargs['minconn'] if 'minconn' in kwargs else 0
        db_access = PgTemplate(db_host=kwargs['db_host'],
                               db_user=kwargs['db_user'],
                               db_password=kwargs['db_password'],
                               db_name=kwargs['db_name'],
                               minconn=min_conn,
                               maxconn=max_conn)

        super(PgORM, self).__init__(db_access)

    def parse_canonical_name(self, tname):
        tname = re.sub('[\s+\"]', '', tname)
        t3 = tname.split('.')
        assert (len(t3) == 3)
        return t3[0], t3[1], t3[2]

    def canonical_name(self, table_catalog, table_schema, table_name):
        return '{}.{}.{}'.format(table_catalog, table_schema, table_name).lower()
