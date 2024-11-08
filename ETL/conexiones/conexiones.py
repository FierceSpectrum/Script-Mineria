"""
Módulo de Conexión a Bases de Datos (SQL Server y Oracle)

Este módulo permite establecer conexiones con bases de datos SQL Server y Oracle
utilizando variables de entorno para la configuración de conexión. La conexión se puede 
realizar mediante `pyodbc` o `SQLAlchemy` para SQL Server y `cx_Oracle` para Oracle.

Funcionalidades principales:
- `mssql_conndb(method, DB_DATABASE)`: Conecta a una base de datos SQL Server utilizando
  `pyodbc` (método 1) o `SQLAlchemy` (método 2).
- `ora_conndb()`: Conecta a una base de datos Oracle utilizando `cx_Oracle`.

Uso:
Para usar este módulo, asegúrate de tener un archivo `.env` con las variables de conexión definidas:
- Para SQL Server: `DB_SERVER`, `DB_DATABASE`, `DB_USERNAME`, `DB_PASSWORD`.
- Para Oracle: `ORACLE_CLIENT_DIR`, `ORA_SERVER`, `ORA_DATABASE`, `ORA_USERNAME`, `ORA_PASSWORD`.

Requerimientos:
- Librerías externas: `pyodbc`, `SQLAlchemy`, `cx_Oracle`, y `python-dotenv`.
"""

# Importación de librerías necesarias
import os
import pyodbc
from sqlalchemy import create_engine, text
import cx_Oracle
from dotenv import load_dotenv

# Carga de variables de entorno desde el archivo .env
load_dotenv()


def mssql_conndb(method=1, DB_DATABASE='DB_DATABASE'):
    """
    Conecta a una base de datos SQL Server usando variables de entorno para la configuración.

    Parámetros:
    - method (int): Selecciona el método de conexión:
        - 1: Conexión usando `pyodbc`.
        - 2: Conexión usando `SQLAlchemy` (con `pyodbc` como controlador).
    - DB_DATABASE (str): Nombre de la variable de entorno para la base de datos a conectar.

    Retorna:
    - Una conexión de `pyodbc.Connection` (método 1) o `SQLAlchemy.Engine` (método 2).

    Excepciones:
    - Levanta un `ValueError` si la variable de entorno `DB_PASSWORD` no está definida.
    """

    # Obtiene valores de conexión de las variables de entorno
    server = os.getenv('DB_SERVER')
    database = os.getenv(DB_DATABASE)
    username = os.getenv('DB_USERNAME')
    password = os.getenv('DB_PASSWORD')

    # Valida si la contraseña está definida
    if not password:
        raise ValueError(
            "La variable de entorno DB_PASSWORD no está definida.")

    # Conexión usando `pyodbc`
    if method == 1:
        conexion_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
        conexion = pyodbc.connect(conexion_str)

    # Conexión usando `SQLAlchemy` y `pyodbc`
    elif method == 2:
        DATABASE_URL = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server'
        conexion = create_engine(DATABASE_URL)

    return conexion


def ora_conndb():
    """
    Conecta a una base de datos Oracle utilizando `cx_Oracle` y variables de entorno para la configuración.

    Pasos:
    1. Inicializa el cliente de Oracle (`cx_Oracle.init_oracle_client`) usando la ruta del cliente
       especificada en `ORACLE_CLIENT_DIR`.
    2. Obtiene los valores de servidor, base de datos, usuario y contraseña de las variables de entorno.
    3. Crea una conexión a Oracle usando `cx_Oracle.connect`.

    Retorna:
    - Una conexión `cx_Oracle.Connection` a la base de datos Oracle.

    Excepciones:
    - Levanta un `ValueError` si la variable de entorno `ORA_PASSWORD` no está definida.
    """
    # Inicializa el cliente Oracle usando la ruta de la variable de entorno `ORACLE_CLIENT_DIR`
    cx_Oracle.init_oracle_client(lib_dir=os.getenv('ORACLE_CLIENT_DIR'))

    # Obtiene valores de conexión de las variables de entorno
    server = os.getenv('ORA_SERVER')
    database = os.getenv('ORA_DATABASE')
    username = os.getenv('ORA_USERNAME')
    password = os.getenv('ORA_PASSWORD')

    # Valida si la contraseña está definida
    if not password:
        raise ValueError(
            "La variable de entorno ORA_PASSWORD no está definida.")

    # Conecta a la base de datos Oracle
    connection = cx_Oracle.connect(
        user=username, password=password, dsn=f"{server}/{database}")
    return connection
