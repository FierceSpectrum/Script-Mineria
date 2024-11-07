import pandas as pd



def check_tables(tables):
    """
    Revisa si se encontraron tablas válidas.

    Esta función valida el parámetro `tables` para asegurarse de que sea una lista con al menos un elemento.
    Si `tables` no es una lista o está vacía, muestra un mensaje de error y termina la ejecución del programa.
    Si la lista es válida, imprime las tablas encontradas.

    Parámetros:
    tables (list): Lista de tablas que se desean verificar.

    Comportamiento:
    - Si `tables` no es una lista o está vacía:
      - Imprime "No se encontraron tablas o el valor no es válido."
      - Termina la ejecución con `exit()`.
    - Si `tables` es una lista no vacía:
      - Imprime "Tablas encontradas: {tables}", donde `{tables}` es la lista de tablas.

    Ejemplo:
    >>> check_tables(['tabla1', 'tabla2'])
    Tablas encontradas: ['tabla1', 'tabla2']
    """
    if not isinstance(tables, list) or len(tables) == 0:
        print("No se encontraron tablas o el valor no es válido.")
        exit()
    print(f"Tablas encontradas: {tables}")


def check_lengths(dataframe, max_length, limit=10):
    """
    Verifica si existen valores en un DataFrame que superen una longitud máxima especificada.

    Esta función revisa cada columna en el DataFrame para identificar valores que exceden el
    número máximo de caracteres (`max_length`). Si encuentra valores largos, imprime el nombre de la columna,
    una muestra limitada de los valores largos y ajusta la visualización de pandas para mostrar todas
    las filas y columnas en los resultados impresos.

    Parámetros:
    dataframe (pd.DataFrame): El DataFrame en el que se buscarán valores largos.
    max_length (int): Longitud máxima permitida para los valores en cada columna.
    limit (int, opcional): Número de filas a mostrar en el caso de encontrar valores largos.
                            Por defecto, es 10.

    Comportamiento:
    - Ajusta las opciones de visualización de pandas para mostrar todas las filas y columnas.
    - Recorre cada columna del DataFrame.
    - Si una columna contiene al menos un valor que excede `max_length`:
        - Imprime el nombre de la columna.
        - Imprime una muestra de hasta `limit` filas con valores que exceden la longitud permitida.
    
    Ejemplo:
    >>> check_lengths(df, max_length=15, limit=5)
    Columna: nombre contiene valores demasiado largos:
         nombre
    0  NombreConMasDe15Caracteres
    2  OtroNombreExtremadamenteLargo
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

    Esta función determina la longitud máxima de los valores en cada columna del DataFrame,
    ordena estas longitudes de mayor a menor y muestra el resultado. Es útil para analizar 
    el tamaño de los datos en cada columna, especialmente cuando se necesita ajustar el formato 
    de las columnas o validar el tamaño de los datos.

    Parámetros:
    df (pd.DataFrame): El DataFrame cuyos tamaños de columna se quieren analizar.

    Comportamiento:
    - Convierte cada valor en el DataFrame a una cadena (str) y calcula la longitud de cada valor.
    - Encuentra el valor máximo de longitud en cada columna.
    - Ordena los tamaños máximos de cada columna en orden descendente.
    - Imprime los tamaños máximos ordenados por columna.

    Ejemplo:
    >>> max_column_sizes(df)
    columna_larga      50
    columna_media      30
    columna_corta      10
    dtype: int64
    """
    
    column_sizes = df.applymap(lambda x: len(str(x))).max()
    sorted_sizes = column_sizes.sort_values(ascending=False)
    print(sorted_sizes)

