import os, sys
from databricks import sql

from qpt_stress_test.db.repositories.databricks_trading import 

def run():

    with sql.connect(server_hostname = "gdt-mo.cloud.databricks.com",       #os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                     http_path       = "/sql/1.0/endpoints/b09bc4bc73bccd24",   #os.getenv("DATABRICKS_HTTP_PATH"),
                     access_token    = "dapi11e376e20349a386f4764df7af20504a") as connection:   #os.getenv("DATABRICKS_TOKEN")

        with connection.cursor() as cursor:
            cursor.execute("WITH ROWS AS (SELECT * FROM qpt.trading_balances_eod LIMIT 2) SELECT * FROM ROWS;")
            result = cursor.fetchall()

        for row in result:
            print(row)

if __name__ == "__main__":
    run()
    sys.exit
