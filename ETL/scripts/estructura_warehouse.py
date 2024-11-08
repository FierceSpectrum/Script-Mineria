"""
Módulo de Creación de Estructura de Tablas en el Data Warehouse

Este módulo permite la creación de tablas en SQL Server para el esquema de un Data Warehouse,
basándose en una configuración dinámica especificada en un archivo JSON. Es útil en flujos ETL
para construir de manera automatizada la estructura de almacenamiento en el Data Warehouse.

Funcionalidad principal:
- `create_tables_wh(connection)`: 
   Lee un archivo JSON de configuración, verifica si cada tabla especificada ya existe en la base de datos,
   y si no, genera y ejecuta la instrucción `CREATE TABLE` correspondiente.

Uso:
Este módulo está diseñado para integrarse en procesos de ETL, permitiendo la creación automática
de tablas en SQL Server a partir de especificaciones externas sin necesidad de escribir SQL manualmente.

Requerimientos:
- Archivo JSON de configuración (`config_warehouse.json`) con las definiciones de las tablas.
- Dependencias de módulos personalizados para conexión y ejecución de consultas SQL.

Ejemplo de uso:
Para importar y utilizar la función de creación de tablas, emplea el siguiente código:

```python
from estructura_warehouse import create_tables_wh
create_tables_wh()
```

Parámetros de configuración:

- create_tables_wh(connection): La función permite pasar una conexión a la base de datos mediante el parámetro connection. 
Si no se especifica, usa target_conn3 como conexión predeterminada. 
"""

# Importación de librerías y módulos necesarios
import sys
# Añade el directorio ETL al path para importar módulos personalizados
sys.path.append("c:/ETLS/Script-Mineria/ETL")

from utilidades import mnd, rlc, tdc, anl, csl, frt
from conexiones import oradbconn, target_conn, target_conn2, target_conn3



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
            print(f"La tabla {name} ya existe. \n")
            continue

        # Genera la instrucción SQL para crear la tabla y la ejecuta
        query = csl.generate_create_table_sql(table)
        csl.execute_sql_query(query, connection)
        print(f"La tabla {name} fue creada. \n")


# Punto de entrada del script
if __name__ == "__main__":
    # Ejecuta el proceso de creación de tablas en el "data warehouse"
    create_tables_wh()
