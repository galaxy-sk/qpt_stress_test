# -*- coding: utf-8 -*-
"""

"""
import datetime as dt
import pytz
import os
from databases import DatabaseURL
from dotenv import load_dotenv

load_dotenv()

ChicagoTimeZone = pytz.timezone('America/Chicago')
NewYorkTimeZone = pytz.timezone('America/New_York')
LATimeZone = pytz.timezone('America/Los_Angeles')
HongKongTimeZone = pytz.timezone('Asia/Hong_Kong')
TokyoTimeZone = pytz.timezone('Asia/Tokyo')

# CLICKHOUSE_HOST = "sv-awoh-md1.na.bluefirecap.net"
CLICKHOUSE_HOST =  os.getenv('CLICKHOUSE_HOST') #"10.17.181.234"   #"10.17.182.249"
CLICKHOUSE_USER =  os.getenv('CLICKHOUSE_USER') #"reader1"

DATABRICKS_SERVER_HOSTNAME  =  os.getenv('DATABRICKS_SERVER_HOSTNAME') # "gdt-mo.cloud.databricks.com" 
DATABRICKS_HTTP_PATH  =  os.getenv('DATABRICKS_HTTP_PATH') # "/sql/1.0/endpoints/b09bc4bc73bccd24"
DATABRICKS_ACCESS_TOKEN  =  os.getenv('DATABRICKS_ACCESS_TOKEN') # "dapi11e376e20349a386f4764df7af20504a"

POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_DATABASE = os.getenv('POSTGRES_DATABASE')
POSTGRES_URL = DatabaseURL(
    #f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}?ssl=true&check_same_thread=false")
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}")

MSSQL_DRIVER = os.getenv('MSSQL_DRIVER')
MSSQL_HOST = os.getenv('MSSQL_HOST')
MSSQL_PORT = os.getenv('MSSQL_PORT')
MSSQL_USER = os.getenv('MSSQL_USER')
MSSQL_PASSWORD = os.getenv('MSSQL_PASSWORD')
MSSQL_DATABASE = os.getenv('MSSQL_DATABASE')
MSSQL_URL = (
    f'DRIVER={MSSQL_DRIVER};SERVER={MSSQL_HOST};PORT={MSSQL_PORT};UID={MSSQL_USER};PWD={MSSQL_PASSWORD};DATABASE={MSSQL_DATABASE};'
    'TrustServerCertificate=Yes')


