"""
Módulo de Formato y Conversión de Datos

Este módulo contiene funciones para formatear, transformar y convertir datos en distintos formatos,
principalmente en `DataFrames` de pandas y tipos de datos SQL para Oracle y SQL Server. Incluye
funciones para leer archivos JSON, transformar datos, y realizar operaciones específicas en listas.

Funcionalidades principales:
- `convert_data_as_dataframe(data, table_name)`: Convierte datos en un `DataFrame` de pandas.
- `read_json(rute)`: Lee un archivo JSON y devuelve su contenido como un diccionario.
- `clear_data_sql(tabla, table_structure, connection)`: Limpia y transforma los datos de una tabla.
- `convert_data_type(data_type, db_type)`: Convierte tipos de datos entre Oracle y SQL Server.
- `data_transform(data, columns, table_structure, transform_func)`: Aplica transformaciones en columnas de datos.
- `dividir_lista_en_sublistas(lista, y)`: Divide una lista en sublistas de tamaño especificado.
- `eliminar_duplicados(lista)`: Elimina los elementos duplicados de una lista manteniendo el orden.

Requerimientos:
- `pandas`: Para manejar `DataFrames`.
- `json`: Para leer archivos JSON.
- Módulo `consultas` para obtener datos de tablas y columnas.
"""


import pandas as pd
import json

from utilidades import csl, tdc
from conexiones import oradbconn, target_conn, target_conn2, target_conn3


def convert_data_as_dataframe(data, table_name):
    """
    Convierte los datos obtenidos de una tabla en un DataFrame de pandas.

    Parámetros:
    - data (list of tuples): Los datos que se desean convertir en un DataFrame. Se espera que sea una lista de tuplas
                           donde cada tupla representa una fila de datos.
    - table_name (str): El nombre de la tabla, utilizado para obtener los nombres de las columnas a través de `table_attributes`.

    Retorno:
    pandas.DataFrame o None: Si los datos y los nombres de las columnas son válidos, retorna un DataFrame de pandas
                              con los datos proporcionados. Si no hay datos o no se pueden obtener los nombres de las
                              columnas, retorna `None`.
    """
    _, column_names = csl.table_attributes(table_name)
    if data and column_names:
        # Convertir los datos en un DtaFrame de pandas
        df = pd.DataFrame(data, columns=column_names)
        return df
    else:
        print(
            f"No hay datos para convertir")
        return None


def read_json(rute="C:/ETLS/Script-Mineria/ETL/config/config_vistas.json"):
    """
    Lee un archivo JSON y devuelve su contenido como un diccionario de Python.

    Parámetros:
    - rute (str): Ruta del archivo JSON que se desea leer. El valor predeterminado es 
                          `"C:/ETLS/Script-Mineria/ETL/config/config_vistas.json"`.

    Retorno:
    dict: Un diccionario con los datos contenidos en el archivo JSON. Si el archivo es inválido o no se puede leer, 
          puede generar una excepción.
    """
    with open(rute, 'r') as file:
        data = json.load(file)
    return data


def clear_data_sql(tabla, table_structure, connection=target_conn):
    """
    Limpia y transforma los datos de una tabla según las configuraciones especificadas.

    Parámetros:
    - tabla (dict): Un diccionario que contiene la información de la tabla, incluida la consulta SQL para obtener los datos 
                  y las configuraciones de transformación (por ejemplo, las columnas que requieren traducción, cambio a 
                  mayúsculas, o asignación de país).
    - table_structure (list of tuples): Una lista de tuplas que contiene las columnas de la tabla y sus tipos de datos, 
                                      necesaria para identificar las columnas a transformar.
    - connection (obj): Conexión activa a la base de datos para ejecutar la consulta SQL (por defecto usa `target_conn`).

    Retorno:
    list: Una lista de tuplas que contiene los datos de la tabla transformados según las configuraciones de la tabla.
    """
    name = tabla['name']
    datos = csl.get_data_query(tabla["query"])

    if not datos:
        print(f"Error obteniendo datos de la tabla {name}")
        return

    list_column_table = [col for col, _ in table_structure]

    # Transformar datos según las columnas configuradas
    if "country" in tabla:
        datos = data_transform(datos, [col.upper() for col in tabla["country"] if col], list_column_table, lambda x: read_json(
            r'C:/ETLS/Script-Mineria/ETL/config/countrys.json').get(x, x))

    if "translate" in tabla:
        datos = tdc.data_translate(
            datos, [col.upper() for col in tabla["translate"] if col], list_column_table)

    if "upper" in tabla:
        datos = data_transform(
            datos, [col.upper() for col in tabla["upper"] if col], list_column_table, str.upper)

    return datos


def convert_data_type(data_type, db_type):
    """
    Convierte tipos de datos entre diferentes bases de datos (Oracle y SQL Server).

    Parámetros:
    - data_type (str): El tipo de dato que se desea convertir. Puede ser un tipo de dato específico de Oracle o SQL Server.
    - db_type (str): El tipo de base de datos a la que se desea convertir el tipo de dato. Puede ser "oracle" o "sqlserver".

    Retorno:
    str: El tipo de dato convertido según la base de datos indicada en `db_type`. Si no se encuentra una conversión 
         específica, retorna el mismo tipo de dato proporcionado.
    """
    if db_type.lower() == "oracle":
        if "INT" in data_type:
            return "NUMBER(10)"
        elif "DECIMAL" in data_type:
            return "NUMBER(15,2)"
        elif "NVARCHAR" in data_type:
            return data_type.replace("NVARCHAR", "VARCHAR2")
        elif "NCHAR" in data_type:
            return data_type.replace("NCHAR", "CHAR")
    elif db_type.lower() == "sqlserver":
        if "NUMBER" in data_type:
            if "(" in data_type and not ("(10)" in data_type):  # Caso de NUMBER con precisión
                return data_type.replace("NUMBER", "DECIMAL")
            return "INT"  # Caso de NUMBER sin precisión
        elif "VARCHAR2" in data_type:
            return data_type.replace("VARCHAR2", "NVARCHAR")
        elif "CHAR" in data_type:
            return data_type.replace("CHAR", "NCHAR")
        elif "nvarchar(-1)" in data_type:
            return data_type.replace("-1", "max")
        # elif "image" in data_type:
        #     return "varbinary(max)"

    return data_type


def data_transform(data, columns, table_structure, transform_func):
    """
    Aplica una transformación a las columnas especificadas de un conjunto de datos.

    Parámetros:
    - data (list of list): Lista de filas de datos, donde cada fila es una lista de valores correspondientes a las columnas.
    - columns (list of str): Lista de nombres de las columnas en las que se desea aplicar la transformación.
    - table_structure (list of str): Lista con los nombres de las columnas de la tabla, que define la estructura de los datos.
    - transform_func (function): Función que se aplicará a cada valor de las columnas especificadas. Esta función debe tomar un valor y devolver el valor transformado.

    Retorno:
    list of list: La lista de datos transformados, con las mismas filas pero con los valores de las columnas especificadas transformados.
    """
    positions = [table_structure.index(col)
                 for col in columns if col in table_structure]
    for file in data:
        for pos in positions:
            if file[pos]:
                file[pos] = transform_func(file[pos])
    return data


def dividir_lista_en_sublistas(lista, y):
    """
    Divide una lista en sublistas de tamaño especificado.

    Parámetros:
    - lista (list): La lista de elementos que se desea dividir en sublistas.
    - y (int): El tamaño de las sublistas. Si el tamaño de la lista no es divisible de manera exacta, 
             la última sublista contendrá los elementos restantes.

    Retorno:
    list: Una lista que contiene las sublistas generadas, donde cada sublista tiene como máximo `y` elementos.
    """
    return [lista[i:i + y] for i in range(0, len(lista), y)]


def eliminar_duplicados(lista):
    """
    Elimina los elementos duplicados de una lista, manteniendo el orden de aparición.

    Parámetros:
    - lista (list): Lista de elementos de cualquier tipo, de la cual se eliminarán los duplicados.

    Retorno:
    list: Una nueva lista con los elementos de la lista original, pero sin duplicados y manteniendo el orden.


    """
    seen = set()
    return [item for item in lista if not (item in seen or seen.add(item))]
