# -*- coding: utf-8 -*-
"""

"""
import pytz
import os
from dotenv import load_dotenv

load_dotenv()

IS_DATABRICKS_ENVIRON = "DATABRICKS_RUNTIME_VERSION" in os.environ

ChicagoTimeZone = pytz.timezone('America/Chicago')
NewYorkTimeZone = pytz.timezone('America/New_York')
LATimeZone = pytz.timezone('America/Los_Angeles')
HongKongTimeZone = pytz.timezone('Asia/Hong_Kong')
TokyoTimeZone = pytz.timezone('Asia/Tokyo')
UtcTimeZone = pytz.UTC

CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER')

DATABRICKS_SERVER_HOSTNAME = os.getenv('DATABRICKS_SERVER_HOSTNAME')
DATABRICKS_HTTP_PATH = os.getenv('DATABRICKS_HTTP_PATH')
DATABRICKS_ACCESS_TOKEN = os.getenv('DATABRICKS_ACCESS_TOKEN')

POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_DATABASE = os.getenv('POSTGRES_DATABASE')
POSTGRES_JDBC_DRIVER = os.getenv('POSTGRES_JDBC_DRIVER')

if IS_DATABRICKS_ENVIRON:
    # Both formats work
    POSTGRES_URL = (
        # f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"
        f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}?user={POSTGRES_USER}&password={POSTGRES_PASSWORD}")
else:
    POSTGRES_URL = (
        # f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"
        # "?ssl=true&check_same_thread=false")
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}")

MSSQL_DRIVER = os.getenv('MSSQL_DRIVER')
MSSQL_HOST = os.getenv('MSSQL_HOST')
MSSQL_PORT = os.getenv('MSSQL_PORT')
MSSQL_USER = os.getenv('MSSQL_USER')
MSSQL_PASSWORD = os.getenv('MSSQL_PASSWORD')
MSSQL_DATABASE = os.getenv('MSSQL_DATABASE')
MSSQL_JDBC_DRIVER = os.getenv('MSSQL_JDBC_DRIVER')

if IS_DATABRICKS_ENVIRON:
    MSSQL_URL = (
        f"jdbc:sqlserver://{MSSQL_HOST}:{MSSQL_PORT};encrypt=true;trustServerCertificate=true;database=Trading;")
else:
    MSSQL_URL = (
        f"DRIVER={MSSQL_DRIVER};SERVER={MSSQL_HOST};PORT={MSSQL_PORT};"
        f"UID={MSSQL_USER};PWD={MSSQL_PASSWORD};DATABASE={MSSQL_DATABASE};"
        "TrustServerCertificate=Yes")
