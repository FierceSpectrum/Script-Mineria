# set DB_SERVER=FREDDY
# set DB_DATABASE=staging
# set DB_USERNAME=sa
# set DB_PASSWORD=Admin12345
# set ORACLE_CLIENT_DIR=C:/oracle/instantclient_21_15
# set ORA_SERVER=127.0.0.1
# set ORA_DATABASE=xe
# set ORA_USERNAME=jardineria
# set ORA_PASSWORD=Admin12345


import os
import pyodbc
from sqlalchemy import create_engine, text
import cx_Oracle
from dotenv import load_dotenv

load_dotenv()


def mssql_conndb(method=1, DB_DATABASE='DB_DATABASE'):
    # Obtener valores de ambiente
    server = os.getenv('DB_SERVER')
    database = os.getenv(DB_DATABASE)
    username = os.getenv('DB_USERNAME')
    password = os.getenv('DB_PASSWORD')
    if not password:
        raise ValueError(
            "La variable de entorno DB_PASSWORD no está definida.")

    if method == 1:
        conexion_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
        conexion = pyodbc.connect(conexion_str)

    if method == 2:
        DATABASE_URL = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server'
        conexion = create_engine(DATABASE_URL)
    return conexion


def ora_conndb():
    cx_Oracle.init_oracle_client(lib_dir=os.getenv('ORACLE_CLIENT_DIR'))

    # Obtener valores de ambiente
    server = os.getenv('ORA_SERVER')
    database = os.getenv('ORA_DATABASE')
    username = os.getenv('ORA_USERNAME')
    password = os.getenv('ORA_PASSWORD')
    if not password:
        raise ValueError(
            "La variable de entorno DB_PASSWORD no está definida.")

    connection = cx_Oracle.connect(
        user=username, password=password, dsn=f"{server}/{database}")
    return connection
