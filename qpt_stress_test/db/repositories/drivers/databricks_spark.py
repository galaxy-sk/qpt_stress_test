
from .interfaces import SqlQueryInterface

class SqlQuery(SqlQueryInterface):
    
    def __init__(self, query: str, *params, db_connector_factory=None):
        self._query = query.format(*params)
        self._params = params
        self._spark = db_connector_factory

    def as_dataframe(self):
        df = self._spark.sql(self._query)
        return df

    def as_instrument_map(self) -> dict:
        with self.db_cursor() as cursor:
            cursor.execute(self._query)
            rs = cursor.fetchall()
            column_names = [col[0] for col in cursor.description]
            map = {
                (row.instrument if "instrument" in column_names else row.symbol_bfc).upper(): {
                    column[0]: value
                    for column, value in zip(cursor.description, row)
                }
                for row in rs
            }     
        return map

    def as_map(self) -> dict:
        with self.db_cursor() as cursor:
            cursor.execute(self._query)
            rs = cursor.fetchall()
            rs_as_map = {
                idx: {
                    column[0]: value
                    for column, value in zip(cursor.description, row)
                }
                for idx, row in enumerate(rs)
            }     
        return rs_as_map

    def as_list(self) -> tuple:
        with self.db_cursor() as cursor:
            cursor.execute(self._query)
            rs = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            data = [[value for value in row] for row in rs]
        return columns, data
