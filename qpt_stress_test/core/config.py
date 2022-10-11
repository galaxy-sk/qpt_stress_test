# -*- coding: utf-8 -*-
"""

"""
from databases import DatabaseURL
from dotenv import load_dotenv
import pytz
import os

load_dotenv()

ChicagoTimeZone = pytz.timezone('America/Chicago')
NewYorkTimeZone = pytz.timezone('America/New_York')
LATimeZone = pytz.timezone('America/Los_Angeles')
HongKongTimeZone = pytz.timezone('Asia/Hong_Kong')
TokyoTimeZone = pytz.timezone('Asia/Tokyo')

POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_DATABASE = os.getenv('POSTGRES_DATABASE')
POSTGRES_URL = DatabaseURL(
    #f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}?ssl=true&check_same_thread=false")
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}")

MSSQL_HOST = os.getenv('MSSQL_HOST')
MSSQL_PORT = os.getenv('MSSQL_PORT')
MSSQL_USER = os.getenv('MSSQL_USER')
MSSQL_PASSWORD = os.getenv('MSSQL_PASSWORD')
MSSQL_DATABASE = os.getenv('MSSQL_DATABASE')
MSSQL_URL = (
    'DRIVER=/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.1.so.1.1;'
    f'SERVER={MSSQL_HOST};PORT={MSSQL_PORT};UID={MSSQL_USER};PWD={MSSQL_PASSWORD};DATABASE={MSSQL_DATABASE};'
    'TrustServerCertificate=Yes')
