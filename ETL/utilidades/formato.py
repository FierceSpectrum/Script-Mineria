import pandas as pd
import json
from utilidades import consultas as cst, traducciones as tdc
from conexiones.conexion_singleton import oradbconn, target_conn, target_conn2, target_conn3


def convert_data_as_dataframe(data, table_name):
    """
    Convierte los datos obtenidos de una tabla en un DataFrame de pandas.

    Esta función toma los datos extraídos de una tabla y los convierte en un DataFrame de pandas. 
    Utiliza los nombres de las columnas obtenidos a partir de la función `table_attributes` para asignar 
    los nombres de las columnas al DataFrame. Si no hay datos o no se pueden obtener los nombres de las columnas, 
    la función devuelve `None`.

    Parámetros:
    data (list of tuples): Los datos que se desean convertir en un DataFrame. Se espera que sea una lista de tuplas
                           donde cada tupla representa una fila de datos.
    table_name (str): El nombre de la tabla, utilizado para obtener los nombres de las columnas a través de `table_attributes`.

    Retorno:
    pandas.DataFrame o None: Si los datos y los nombres de las columnas son válidos, retorna un DataFrame de pandas
                              con los datos proporcionados. Si no hay datos o no se pueden obtener los nombres de las
                              columnas, retorna `None`.

    Comportamiento:
    - Llama a la función `table_attributes` para obtener los nombres de las columnas de la tabla.
    - Si hay datos y los nombres de las columnas son válidos, crea un DataFrame de pandas usando esos datos y columnas.
    - Si no hay datos o los nombres de las columnas no se pueden obtener, imprime un mensaje de advertencia y retorna `None`.

    Excepciones:
    - Si ocurre algún error al convertir los datos en un DataFrame, se captura y se muestra el mensaje correspondiente.
    
    Ejemplo:
    >>> data = [(1, 'Juan', 'Perez'), (2, 'Ana', 'Lopez')]
    >>> convert_data_as_dataframe(data, "empleados")
    >>> # DataFrame con las columnas y datos correspondientes a la tabla "empleados"
    """
    _, column_names = cst.table_attributes(table_name)
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

    Esta función abre y lee un archivo JSON especificado por su ruta (`rute`). El contenido del archivo 
    se carga en un diccionario de Python utilizando el módulo `json` de la biblioteca estándar, y se retorna 
    el diccionario con los datos.

    Parámetros:
    rute (str, opcional): Ruta del archivo JSON que se desea leer. El valor predeterminado es 
                          `"C:/ETLS/Script-Mineria/ETL/config/config_vistas.json"`.
                          
    Retorno:
    dict: Un diccionario con los datos contenidos en el archivo JSON. Si el archivo es inválido o no se puede leer, 
          puede generar una excepción.

    Comportamiento:
    - Abre el archivo JSON en modo de lectura (`'r'`).
    - Utiliza la función `json.load()` para leer y cargar el contenido del archivo en un diccionario.
    - Retorna el diccionario con los datos del archivo.

    Excepciones:
    - Si el archivo no existe o no se puede abrir, Python lanzará una excepción de tipo `FileNotFoundError`.
    - Si el contenido del archivo no es un JSON válido, se lanzará una excepción de tipo `json.JSONDecodeError`.

    Ejemplo:
    >>> read_json("config/config_vistas.json")
    >>> # Retorna el contenido del archivo JSON como un diccionario de Python
    """
    with open(rute, 'r') as file:
        data = json.load(file)
    return data


def clear_data_sql(tabla, table_structure, connection=target_conn):
    """
    Limpia y transforma los datos de una tabla según las configuraciones especificadas.

    Esta función obtiene los datos de una tabla a través de una consulta SQL y luego aplica varias transformaciones
    a esos datos basadas en la configuración de la tabla. Las transformaciones pueden incluir cambios en los valores de
    ciertas columnas, como la traducción de valores, la transformación de textos a mayúsculas, o la asignación de valores
    a partir de un archivo JSON (por ejemplo, para el campo "country"). Luego de aplicar las transformaciones, retorna los
    datos transformados.

    Parámetros:
    tabla (dict): Un diccionario que contiene la información de la tabla, incluida la consulta SQL para obtener los datos 
                  y las configuraciones de transformación (por ejemplo, las columnas que requieren traducción, cambio a 
                  mayúsculas, o asignación de país).
    table_structure (list of tuples): Una lista de tuplas que contiene las columnas de la tabla y sus tipos de datos, 
                                      necesaria para identificar las columnas a transformar.
    connection (obj, opcional): Conexión activa a la base de datos para ejecutar la consulta SQL (por defecto usa `target_conn`).

    Retorno:
    list: Una lista de tuplas que contiene los datos de la tabla transformados según las configuraciones de la tabla.

    Comportamiento:
    - Llama a la función `get_data_query` para obtener los datos de la tabla a partir de la consulta SQL proporcionada en el diccionario `tabla`.
    - Si hay transformaciones configuradas en el diccionario `tabla`, las aplica a los datos obtenidos.
        - Si la clave "country" está presente en `tabla`, se transforman los valores de las columnas indicadas según el archivo JSON de países.
        - Si la clave "translate" está presente en `tabla`, se aplican transformaciones de traducción de valores de columna.
        - Si la clave "upper" está presente en `tabla`, los valores de las columnas correspondientes se convierten a mayúsculas.
    - Retorna los datos transformados.

    Excepciones:
    - Si no se obtienen datos de la tabla (es decir, la consulta devuelve `None` o una lista vacía), se imprime un mensaje de error.
    
    Ejemplo:
    >>> tabla_config = {
    >>>     "name": "empleados",
    >>>     "query": "SELECT * FROM empleados",
    >>>     "country": ["pais"],
    >>>     "translate": ["nombre"],
    >>>     "upper": ["apellido"]
    >>> }
    >>> table_structure = [("id", "INT"), ("nombre", "VARCHAR"), ("pais", "VARCHAR"), ("apellido", "VARCHAR")]
    >>> clear_data_sql(tabla_config, table_structure)
    >>> # Retorna los datos transformados según las configuraciones indicadas en `tabla_config`
    """
    name = tabla['name']
    datos = cst.get_data_query(tabla["query"])

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

    Esta función toma un tipo de dato de una base de datos y lo convierte al tipo de dato correspondiente 
    en una base de datos de tipo Oracle o SQL Server, según se indique en el parámetro `db_type`. La conversión 
    se basa en las diferencias de nomenclatura y formatos entre Oracle y SQL Server.

    Parámetros:
    data_type (str): El tipo de dato que se desea convertir. Puede ser un tipo de dato específico de Oracle o SQL Server.
    db_type (str): El tipo de base de datos a la que se desea convertir el tipo de dato. Puede ser "oracle" o "sqlserver".

    Retorno:
    str: El tipo de dato convertido según la base de datos indicada en `db_type`. Si no se encuentra una conversión 
         específica, retorna el mismo tipo de dato proporcionado.

    Comportamiento:
    - Si `db_type` es "oracle":
        - Convierte tipos de datos de SQL Server a los equivalentes de Oracle, como "INT" a "NUMBER(10)" y 
          "VARCHAR" a "VARCHAR2".
    - Si `db_type` es "sqlserver":
        - Convierte tipos de datos de Oracle a los equivalentes de SQL Server, como "NUMBER" a "INT" o "DECIMAL", 
          y "VARCHAR2" a "NVARCHAR".
    - En todos los casos, la función trata de encontrar coincidencias para tipos de datos específicos (por ejemplo, "DECIMAL", "CHAR") 
      y los ajusta según las reglas de la base de datos correspondiente.

    Excepciones:
    - No hay excepciones explícitas. Si no se encuentra un tipo de dato que coincida con las reglas de conversión, 
      la función simplemente retorna el `data_type` sin modificaciones.

    Ejemplo:
    >>> convert_data_type("NVARCHAR", "oracle")
    >>> # Retorna "VARCHAR2" para Oracle.

    >>> convert_data_type("NUMBER(10)", "sqlserver")
    >>> # Retorna "INT" para SQL Server.
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

    Esta función recorre las filas de datos y aplica una función de transformación a las columnas 
    específicas indicadas en el parámetro `columns`, de acuerdo con la estructura de la tabla dada.

    Parámetros:
    data (list of list): Lista de filas de datos, donde cada fila es una lista de valores correspondientes a las columnas.
    columns (list of str): Lista de nombres de las columnas en las que se desea aplicar la transformación.
    table_structure (list of str): Lista con los nombres de las columnas de la tabla, que define la estructura de los datos.
    transform_func (function): Función que se aplicará a cada valor de las columnas especificadas. Esta función debe tomar un valor y devolver el valor transformado.

    Retorno:
    list of list: La lista de datos transformados, con las mismas filas pero con los valores de las columnas especificadas transformados.

    Comportamiento:
    - La función primero determina las posiciones (índices) de las columnas que se deben transformar, basándose en la 
      `table_structure` y las columnas especificadas.
    - Luego, recorre cada fila en los datos y aplica la función de transformación a los valores de las columnas seleccionadas.
    - Si el valor de una celda no es `None` o vacío, se le aplica la transformación especificada.

    Ejemplo:
    >>> data = [
    >>>     ["John", "doe", "USA"],
    >>>     ["Jane", "doe", "Canada"]
    >>> ]
    >>> columns = ["name"]
    >>> table_structure = ["name", "surname", "country"]
    >>> data_transform(data, columns, table_structure, str.upper)
    >>> # Retorna:
    >>> # [
    >>> #     ["JOHN", "doe", "USA"],
    >>> #     ["JANE", "doe", "Canada"]
    >>> # ]
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

    Esta función toma una lista de elementos y la divide en sublistas de tamaño `y`. Si la lista no puede ser
    dividida de manera exacta, la última sublista tendrá los elementos restantes.

    Parámetros:
    lista (list): La lista de elementos que se desea dividir en sublistas.
    y (int): El tamaño de las sublistas. Si el tamaño de la lista no es divisible de manera exacta, 
             la última sublista contendrá los elementos restantes.

    Retorno:
    list: Una lista que contiene las sublistas generadas, donde cada sublista tiene como máximo `y` elementos.

    Comportamiento:
    - La función recorre la lista original y agrupa los elementos en sublistas del tamaño especificado por `y`.
    - Si la longitud de la lista no es un múltiplo exacto de `y`, la última sublista puede contener menos de `y` elementos.
    
    Ejemplo:
    >>> dividir_lista_en_sublistas([1, 2, 3, 4, 5, 6, 7, 8], 3)
    >>> # Retorna: [[1, 2, 3], [4, 5, 6], [7, 8]]
    """
    return [lista[i:i + y] for i in range(0, len(lista), y)]


def eliminar_duplicados(lista):
    """
    Elimina los elementos duplicados de una lista, manteniendo el orden de aparición.

    Esta función recorre la lista original y elimina cualquier elemento que se repita, dejando solo
    la primera ocurrencia de cada elemento. El orden de los elementos se conserva tal como aparecen
    en la lista original.

    Parámetros:
    lista (list): Lista de elementos de cualquier tipo, de la cual se eliminarán los duplicados.

    Retorno:
    list: Una nueva lista con los elementos de la lista original, pero sin duplicados y manteniendo el orden.

    Comportamiento:
    - La función recorre la lista original y utiliza un conjunto (`set`) para realizar un seguimiento
      de los elementos que ya han aparecido.
    - Solo la primera ocurrencia de cada elemento se mantiene en la nueva lista; las ocurrencias posteriores
      se eliminan.

    Ejemplo:
    >>> eliminar_duplicados([1, 2, 2, 3, 4, 4, 5])
    >>> # Retorna: [1, 2, 3, 4, 5]
    """
    seen = set()
    return [item for item in lista if not (item in seen or seen.add(item))]
