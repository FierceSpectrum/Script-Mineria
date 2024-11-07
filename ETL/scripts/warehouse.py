"""
Módulo de Creación de Tablas en SQL Server

Este módulo proporciona funciones para generar y ejecutar instrucciones SQL
que crean tablas en una base de datos SQL Server, basándose en la definición 
de las tablas proporcionadas en un archivo de configuración JSON. 

Funcionalidades principales:
- `generate_create_table_sql(table_json)`: 
   Genera la instrucción SQL de creación de tabla en base a la configuración JSON de la tabla.
- `create_tables_wh(connection)`: 
   Lee la configuración de las tablas desde un archivo JSON y verifica si cada tabla
   ya existe en la base de datos. Si no existe, genera la instrucción `CREATE TABLE` 
   correspondiente y la ejecuta.

El archivo está diseñado para integrarse en flujos ETL en los que se necesita construir
un esquema de tablas en un "Data Warehouse" basado en especificaciones dinámicas. 

Requerimientos:
- Configuración JSON (`config_warehouse.json`) con detalles de cada tabla a crear.
- Dependencias de módulos personalizados para la conexión a base de datos y utilidades de
  formato, consultas, y validación de datos.

Uso:
Este módulo puede ser importado en otros scripts de ETL para automatizar la creación de tablas 
en SQL Server al ser llamado de la siguiente forma:
   
```python
from nombre_del_modulo import create_tables_wh
create_tables_wh()
```

Parámetros de configuración:

- La función create_tables_wh permite establecer una conexión a la base de datos mediante el parámetro connection, el cual, si no se especifica, utiliza target_conn3 como conexión predeterminada.
Este módulo es útil en casos donde es necesario configurar y crear tablas dinámicamente basadas en especificaciones externas sin necesidad de editar el SQL manualmente.


"""
import sys
sys.path.append("c:/ETLS/Script-Mineria/ETL")

from utilidades import analisis as anl, consultas as csl, formato as frt
from utilidades import manejo_datos as mnd, relaciones as rlc, traducciones as tdc
from conexiones.conexion_singleton import oradbconn, target_conn, target_conn2, target_conn3


def generate_create_table_sql(table_json):
    """
    Genera una instrucción SQL para crear una tabla en SQL Server basada en una
    configuración JSON que describe la estructura de la tabla.

    Parámetros:
    - table_json (dict): JSON con los detalles de la tabla, incluyendo nombre,
      columnas y claves primarias.

    Retorna:
    - str: una cadena con la instrucción SQL `CREATE TABLE`.
    """

    # Extrae el nombre de la tabla desde el JSON y comienza la instrucción SQL
    table_name = table_json["name"]
    sql = f'CREATE TABLE "{table_name}" (\n'

    # Definición de columnas
    columns = []
    for column in table_json["columns"]:
        # Obtiene el nombre y tipo de la columna
        col_def = f'"{column["name"]}" {column["type"].upper()}'

        # Añade la longitud para tipos de datos como nvarchar o varchar
        if "length" in column:
            col_def += f'({column["length"]})'

        # Configura la columna como autoincremental si tiene la opción identity
        if "identity" in column:
            seed = column["identity"].get("seed", 1)
            increment = column["identity"].get("increment", 1)
            col_def += f' IDENTITY({seed}, {increment})'

        # Define si la columna permite valores NULL
        if not column.get("nullable", True):
            col_def += " NOT NULL"

        # Añade la definición de columna a la lista de columnas
        columns.append(col_def)

    # Combina todas las columnas en la instrucción SQL
    sql += ",\n".join(columns)

    # Añade la clave primaria si está definida en el JSON
    primary_keys = [col["name"]
                    for col in table_json["columns"] if col.get("primaryKey", False)]
    if primary_keys:
        ands = ", ".join(f'"{pk}"' for pk in primary_keys)
        sql += f',\nPRIMARY KEY ({ands})'

    # Completa la instrucción SQL
    sql += "\n);"
    return sql


def create_tables_wh(connection=target_conn3):
    """
    Lee un archivo de configuración JSON que describe las tablas a crear y,
    para cada tabla que no exista en la base de datos de destino, ejecuta una
    instrucción SQL `CREATE TABLE`.

    Parámetros:
    - connection: conexión a la base de datos (por defecto es target_conn3).
    """
    # Carga el archivo de configuración JSON que contiene los detalles de las tablas
    tables_warehouse = frt.read_json(
        "C:/ETLS/Script-Mineria/ETL/config/config_warehouse.json")

    # Itera sobre cada tabla en el JSON de configuración
    for table in tables_warehouse["tables"]:
        name = table["name"]

        # Verifica si la tabla ya existe en la base de datos
        exist_table = csl.table_exists(name, "dbo", connection=connection)
        if exist_table:
            print(f"La tabla {name} ya existe.")
            continue

        # Genera la instrucción SQL para crear la tabla y la ejecuta
        query = generate_create_table_sql(table)
        csl.execute_sql_query(query, connection)
        print(f"La tabla {name} fue creada.")


# Punto de entrada del script
if __name__ == "__main__":
    # Ejecuta el proceso de creación de tablas en el "data warehouse"
    create_tables_wh()
