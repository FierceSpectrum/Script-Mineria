"""
Módulo de Extracción y Migración de Datos

Este módulo está diseñado para migrar datos desde diferentes bases de datos (Oracle y SQL Server)
a otra base de datos SQL Server. Permite extraer datos, crear tablas de destino y 
transferir los datos entre las bases.

Funcionalidades principales:
- `migrate_sql_to_sql(connection)`: Migra datos desde una base de datos SQL Server
   a otra instancia de SQL Server.
- `migrate_oracle_to_sql()`: Migra datos desde una base de datos Oracle a una base de datos SQL Server.

Cada función realiza los siguientes pasos:
1. Obtiene la lista de tablas de la base de datos de origen.
2. Verifica si la tabla existe en la base de datos de destino.
3. Si la tabla no existe en el destino, crea su estructura en la base de datos de destino.
4. Si la tabla tiene datos, limpia la tabla de destino y carga los datos extraídos.

Uso:
Este módulo está diseñado para ejecutarse de forma independiente como script.
Cuando se ejecuta directamente, realiza migraciones desde ambas bases de datos (Oracle y SQL Server)
al servidor SQL Server de destino especificado.

Requerimientos:
- Conexiones a bases de datos (configuradas en `conexion_singleton`).
- Módulos de utilidades para manejo de datos, consultas, análisis y formatos.
"""

import sys
# Añade el directorio ETL al path para importar módulos personalizados
sys.path.append("c:/ETLS/Script-Mineria/ETL")

from conexiones.conexion_singleton import oradbconn, target_conn, target_conn2, target_conn3
from utilidades import manejo_datos as mnd, relaciones as rlc, traducciones as tdc
from utilidades import analisis as anl, consultas as csl, formato as frt


def migrate_sql_to_sql(connection=target_conn2):
    """
    Migra datos de una instancia de SQL Server a otra instancia de SQL Server.

    Parámetros:
    - connection: Conexión a la base de datos de origen SQL Server.

    Pasos de la función:
    1. Obtiene la lista de tablas de la base de datos de origen usando `csl.get_user_tables`.
    2. Verifica que las tablas obtenidas existan en la base de datos de destino con `anl.check_tables`.
    3. Itera por cada tabla y:
       a. Verifica si la tabla existe en la base de datos de destino con `csl.table_exists`.
       b. Si no existe, obtiene su estructura de origen y crea la tabla en destino con `mnd.create_table`.
       c. Obtiene los datos de la tabla de origen usando `csl.get_table_data`.
       d. Si la tabla existe y tiene datos:
          - Limpia los datos existentes en la tabla destino (usando `TRUNCATE`).
          - Inserta los datos en la tabla de destino con `mnd.add_data_entity`.
       e. Si no se obtienen datos, imprime un mensaje de error.
    """

    # Lista de tablas en la base de datos de origen
    tablas = csl.get_user_tables("sqlserver", target_conn2)
    # Verifica la existencia de tablas en destino
    anl.check_tables(tablas)

    for tabla in tablas:

        # Verifica si la tabla existe en destino
        exists = csl.table_exists(tabla, "dbo")
        # Extrae datos de la tabla en origen
        datos = csl.get_table_data('dbo', tabla, target_conn2)

        if not exists:
            # Obtiene la estructura de la tabla de origen
            table_structure = csl.get_table_structure(
                tabla, "dbo", "sqlserver", target_conn2)
            # Crea la tabla en destino
            mnd.create_table(tabla, "dbo", table_structure)
            # Re-verifica la existencia de la tabla en destino
            exists = csl.table_exists(tabla, "dbo")

        if datos and exists:
            # Elimina los datos existentes en la tabla destino
            mnd.delete_data_entity(tabla, 'TRUNCATE')
            # Cuenta las columnas de la tabla de origen
            cant_columns = csl.count_columns(tabla, "sqlserver", target_conn2)
            # Inserta los datos en la tabla destino
            mnd.add_data_entity(datos, tabla, cant_columns)
        else:
            # Mensaje de error si no se obtienen datos
            print(f"Error obteniendo datos de la tabla {tabla}")

        print("\n")  # Espacio en blanco para separar logs de cada tabla


def migrate_oracle_to_sql():
    """
    Migra datos de una base de datos Oracle a una base de datos SQL Server.

    Pasos de la función:
    1. Obtiene la lista de tablas de la base de datos Oracle de origen.
    2. Verifica que las tablas obtenidas existan en la base de datos de destino.
    3. Itera por cada tabla y:
       a. Verifica si la tabla existe en la base de datos de destino.
       b. Si no existe, obtiene su estructura de la base Oracle y crea la tabla en destino.
       c. Obtiene los datos de la tabla de origen.
       d. Si la tabla existe y tiene datos:
          - Limpia los datos existentes en la tabla destino (usando `TRUNCATE`).
          - Inserta los datos en la tabla de destino.
       e. Si no se obtienen datos, imprime un mensaje de error.
    """
    tablas = csl.get_user_tables()  # Lista de tablas en la base de datos Oracle
    anl.check_tables(tablas)  # Verifica que las tablas de origen existan

    for tabla in tablas:
        # Verifica si la tabla existe en destino
        exists = csl.table_exists(tabla, "dbo")
        # Extrae datos de la tabla en Oracle
        datos = csl.get_table_data('jardineria', tabla)

        if not exists:
            # Obtiene la estructura de la tabla de origen en Oracle
            table_structure = csl.get_table_structure(tabla, "JARDINERIA")
            # Crea la tabla en destino
            mnd.create_table(tabla, "dbo", table_structure)
            # Re-verifica la existencia de la tabla en destino
            exists = csl.table_exists(tabla, "dbo")

        if datos and exists:
            # Elimina los datos existentes en la tabla destino
            mnd.delete_data_entity(tabla, 'TRUNCATE')
            # Cuenta las columnas de la tabla de origen
            cant_columns = csl.count_columns(tabla)
            # Inserta los datos en la tabla destino
            mnd.add_data_entity(datos, tabla, cant_columns)
        else:
            # Mensaje de error si no se obtienen datos
            print(f"Error obteniendo datos de la tabla {tabla}")

        print("\n")  # Espacio en blanco para separar logs de cada tabla


# Punto de entrada principal del módulo
if __name__ == "__main__":
    # Ejecuta ambas funciones de migración al ejecutar el script directamente
    migrate_oracle_to_sql()
    migrate_sql_to_sql()
