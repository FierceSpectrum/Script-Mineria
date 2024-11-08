"""
Módulo de Análisis de Tablas y DataFrames

Este módulo proporciona funciones para realizar verificaciones y análisis 
básicos en listas de tablas y en objetos `DataFrame` de pandas. Incluye 
funciones para validar la existencia de tablas, verificar la longitud de 
valores en columnas y obtener el tamaño máximo de cada columna en un 
`DataFrame`.

Funcionalidades principales:
- `check_tables(tables)`: Verifica si la lista de tablas proporcionada 
  contiene elementos válidos y los muestra.
- `check_lengths(dataframe, max_length, limit)`: Examina cada columna en 
  un `DataFrame` para detectar valores que exceden una longitud máxima 
  especificada, y los muestra hasta un límite de filas.
- `max_column_sizes(df)`: Calcula y muestra el tamaño máximo de los valores 
  en cada columna de un `DataFrame`.

Uso:
Este módulo está pensado para ser utilizado en otros scripts donde se 
requiera realizar verificaciones rápidas y análisis de estructura sobre 
`DataFrames` en proyectos de ETL u otros procesos de manejo de datos.

Requerimientos:
- `pandas`: Todas las funciones en este módulo utilizan `DataFrame` de 
  pandas para la manipulación de datos.
"""

import pandas as pd


def check_tables(tables):
    """
    Revisa si se encontraron tablas válidas.

    Parámetros:
    - tables (list): Lista de tablas que se desean verificar. 
    """
    if not isinstance(tables, list) or len(tables) == 0:
        print("No se encontraron tablas o el valor no es válido.")
        exit()
    print(f"Tablas encontradas: {tables}")


def check_lengths(dataframe, max_length, limit=10):
    """
    Verifica si existen valores en un DataFrame que superen una longitud máxima especificada.

    Parámetros:
    - dataframe (pd.DataFrame): El DataFrame en el que se buscarán valores largos.
    max_length (int): Longitud máxima permitida para los valores en cada columna.
    limit (int, opcional): Número de filas a mostrar en el caso de encontrar valores largos.
                            Por defecto, es 10.

    """

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    for column in dataframe.columns:
        if dataframe[column].apply(lambda x: len(str(x)) > max_length).any():
            print(f"Columna: {column} contiene valores demasiado largos:")
            print(dataframe[dataframe[column].apply(
                lambda x: len(str(x)) > max_length)].head(limit))
            print("\n")


def max_column_sizes(df):
    """
    Calcula y muestra el tamaño máximo de los valores en cada columna de un DataFrame.

    Parámetros:
    - df (pd.DataFrame): El DataFrame cuyos tamaños de columna se quieren analizar.
    """

    column_sizes = df.applymap(lambda x: len(str(x))).max()
    sorted_sizes = column_sizes.sort_values(ascending=False)
    print(sorted_sizes)
