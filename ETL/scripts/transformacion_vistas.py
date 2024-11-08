"""
Módulo de Transformación y Limpieza de Vistas

Este módulo está diseñado para limpiar, transformar y exportar vistas en SQL Server de acuerdo 
con un archivo de configuración JSON. Cada vista es generada o actualizada en la base de datos, 
con la posibilidad de crear tablas auxiliares donde se almacenen datos limpios antes de 
actualizar las vistas originales.

Funcionalidades principales:
- `clear_views(connection)`: Limpia y transforma los datos de las vistas especificadas en 
  el archivo JSON, almacenando los datos en tablas auxiliares y actualizando las vistas 
  en SQL Server.

Uso:
Este módulo está diseñado para ejecutarse de forma independiente como script o puede ser 
importado en otros scripts para realizar la limpieza y transformación de vistas mediante la 
función `clear_views`.

Requerimientos:
- Configuración JSON (`config_vistas.json`) con la lista de vistas y sus consultas SQL asociadas.
- Conexiones a las bases de datos (configuradas en `conexion_singleton`).
- Módulos de utilidades para manejo de datos, consultas, análisis y relaciones.
"""

# Importación de librerías y módulos necesarios
import sys
# Añade el directorio ETL al path para importar módulos personalizados
sys.path.append("c:/ETLS/Script-Mineria/ETL")

from utilidades import mnd, rlc, tdc, anl, csl, frt
from conexiones import oradbconn, target_conn, target_conn2, target_conn3


def clear_views(connection=target_conn):
    """
    Limpia y transforma datos de las vistas en SQL Server según las definiciones del archivo JSON.

    Parámetros:
    - connection: Conexión a la base de datos SQL Server donde se ejecutarán las transformaciones
      (por defecto `target_conn`).

    Pasos de la función:
    1. Lee la configuración de vistas desde el archivo JSON `config_vistas.json`.
    2. Para cada vista definida en el JSON:
       a. Verifica si la vista existe en la base de datos de SQL Server.
       b. Si la vista no existe, crea una nueva vista a partir de la consulta SQL definida en el JSON.
       c. Obtiene la estructura de la vista y crea una tabla auxiliar (`CLEAR_<nombre de la vista>`) para almacenar los datos limpios.
       d. Verifica si la estructura de la tabla auxiliar coincide con la estructura de la vista, y si no es así, omite la transformación.
       e. Ejecuta la limpieza y transformación de datos según las reglas del archivo JSON.
       f. Inserta los datos limpios en la tabla auxiliar, actualiza la vista original para reflejar los datos de la tabla auxiliar.
    """

    # Carga el archivo JSON con la configuración de vistas y su transformación
    datos = frt.read_json(
        r'c:/ETLS/Script-Mineria/ETL/config/config_vistas.json')
    tablas = datos["views"]

    # Itera sobre cada vista definida en el JSON
    for tabla in tablas:
        name = tabla['name'].upper()  # Nombre de la vista en mayúsculas
        # Nombre de la tabla auxiliar para almacenamiento limpio
        table_name = f"CLEAR_{name}"

        # Verifica si la vista existe en SQL Server
        exists = csl.table_exists(name, "dbo")
        if not exists:
            # Crea o altera la vista a partir de la consulta SQL en el JSON
            view_query = f"CREATE OR ALTER VIEW {name} AS SELECT * FROM ({tabla['query']}) AS V;"
            mnd.execute_sql_view(view_query)
            exists = csl.table_exists(name, "dbo")

        # Obtiene la estructura de la vista de SQL Server
        table_structure = csl.get_table_structure(
            name, "dbo", "sqlserver", connection)

        # Verifica si la tabla auxiliar existe, y si no, la crea con la estructura de la vista
        exists = csl.table_exists(table_name, "dbo")
        if not exists:
            mnd.create_table(table_name, "dbo", table_structure)
            exists = csl.table_exists(table_name, "dbo")
        else:
            # Verifica si la estructura de la tabla auxiliar coincide con la de la vista
            table_structure2 = csl.get_table_structure(
                table_name, "dbo", "sqlserver", connection)
            if table_structure != table_structure2:
                print(
                    f"No se pueden insertar datos ya que la estructura de las tablas {name} y {table_name} no coinciden")
                print("\n")
                continue

        # Ejecuta la limpieza y transformación de los datos
        datos = frt.clear_data_sql(tabla, table_structure, connection)

        # Si hay datos y la tabla auxiliar existe, procede con la inserción de datos limpios
        if datos and exists:
            # Limpia los datos antiguos de la tabla auxiliar
            mnd.delete_data_entity(table_name, 'TRUNCATE')
            cant_columns = csl.count_columns(
                table_name, "sqlserver", connection)  # Cuenta las columnas
            # Inserta los datos limpios en la tabla auxiliar
            mnd.add_data_entity(datos, table_name, cant_columns)

            # Actualiza la vista para reflejar los datos de la tabla auxiliar
            view_query = f"CREATE OR ALTER VIEW {name} AS SELECT * FROM {table_name};"
            mnd.execute_sql_view(view_query)

        print("\n")  # Espacio para separar logs de cada vista


# Punto de entrada principal del módulo
if __name__ == "__main__":
    # Ejecuta la limpieza y transformación de vistas al ejecutar el script
    clear_views()
