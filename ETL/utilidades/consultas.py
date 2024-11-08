"""
Módulo de Consultas SQL para Bases de Datos

Este módulo proporciona funciones para realizar operaciones SQL en bases de datos SQL Server y Oracle.
Incluye funciones para verificar la existencia de tablas, obtener la estructura de una tabla, ejecutar
consultas SQL, crear instrucciones de inserción, y administrar relaciones de clave foránea entre tablas.

Funcionalidades principales:
- `table_exists(table_name, owner, db_type, connection)`: Verifica si una tabla existe en la base de datos.
- `get_table_structure(table_name, owner, db_type, connection)`: Obtiene la estructura de columnas de una tabla.
- `get_data_query(sql_query, connection)`: Ejecuta una consulta SQL y devuelve los resultados.
- `execute_sql_query(sql_query, connection)`: Ejecuta una consulta SQL y confirma los cambios en la base de datos.
- `create_sql_select(owner, table_name, columns)`: Genera una consulta SQL `SELECT` para una tabla específica.
- `create_sql_view(view_name, view_columns, data)`: Genera una instrucción SQL para crear o modificar una vista.
- `get_table_data(owner, table_name, connection)`: Obtiene todos los datos de una tabla en la base de datos.
- `create_sql_insert(owner, table, numfields)`: Genera una instrucción SQL `INSERT` para una tabla específica.
- `count_columns(table, db_type, connection)`: Cuenta el número de columnas de una tabla en una base de datos.
- `get_user_tables(db_type, connection)`: Obtiene una lista de las tablas del usuario en la base de datos.
- `table_attributes(table_name, db_type, connection)`: Obtiene los nombres y cantidad de columnas de una tabla.
- `disable_restrictions(table_name, connection)`: Deshabilita las restricciones de una tabla en SQL Server.
- `enable_restrictions(table_name, connection)`: Habilita las restricciones de una tabla en SQL Server.
- `generate_create_table_sql(table_json)`: Genera la instrucción SQL de creación de tabla en base a la configuración JSON de la tabla.

Requerimientos:
- Conexión a bases de datos mediante `conexion_singleton` para ejecutar las consultas SQL.
"""

from conexiones import oradbconn, target_conn, target_conn2, target_conn3


def table_exists(table_name, owner, db_type="sqlserver", connection=target_conn):
    """
    Verifica si una tabla existe en una base de datos específica.

    Parámetros:
    - table_name (str): Nombre de la tabla que se desea verificar.
    - owner (str): Esquema o propietario de la tabla.
    - db_type (str, ): Tipo de base de datos, que puede ser 'sqlserver' (por defecto) u 'oracle'.
    - connection (obj, ): Conexión activa a la base de datos.

    Retorno:
    bool: Devuelve `True` si la tabla existe, y `False` si no.
    """

    cursor = connection.cursor()
    try:
        if db_type.lower() == "oracle":
            check_query = f"SELECT COUNT(*) FROM all_tables WHERE owner = '{owner.upper()}' AND table_name = '{table_name.upper()}'"
        elif db_type.lower() == "sqlserver":
            check_query = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{owner}' AND TABLE_NAME = '{table_name}'"
        else:
            raise ValueError(
                "Tipo de base de datos no soportado. Use 'oracle' o 'sqlserver'.")

        cursor.execute(check_query)
        table_exists = cursor.fetchone()[0]

        if table_exists > 0:
            print(f"La tabla {table_name} existe en {owner}.")
            return True
        else:
            print(f"La tabla {table_name} no existe en {owner}.")
            return False

    except Exception as e:
        print(f"Error al verificar la tabla: {str(e)}")
        return False

    finally:
        if cursor:
            cursor.close()


def get_table_structure(table_name, owner, db_type="oracle", connection=oradbconn):
    """
    Obtiene la estructura de columnas de una tabla específica en una base de datos.

    Parámetros:
    - table_name (str): Nombre de la tabla cuyo esquema se quiere obtener.
    - owner (str): Esquema o propietario de la tabla.
    - db_type (str, ): Tipo de base de datos, que puede ser 'oracle' (por defecto) o 'sqlserver'.
    - connection (obj, ): Conexión activa a la base de datos.

    Retorno:
    list: Lista de tuplas con la estructura de la tabla. Cada tupla contiene:
          (nombre_columna, tipo_dato) o (nombre_columna, tipo_dato(tamaño)) para datos de longitud variable.
    """

    cursor = connection.cursor()
    try:
        # Obtener las columnas y tipos de datos de la tabla fuente
        if db_type.lower() == "oracle":
            column_query = f"""
                SELECT column_name, data_type, data_length, data_precision
                FROM all_tab_columns
                WHERE owner = '{owner.upper()}' AND table_name = '{table_name.upper()}'
            """
        elif db_type.lower() == "sqlserver":
            column_query = f"""
                SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{owner}' AND TABLE_NAME = '{table_name}'
            """

        # Ejecutar la consulta para obtener columnas y tipos de datos
        cursor.execute(column_query)
        columns = cursor.fetchall()

        if not columns:
            raise ValueError(
                f"No se pudieron obtener las columnas de la tabla {table_name} en {owner}.")

        # Preparar el resultado con columnas, tipos y longitudes
        table_structure = []
        for column_name, data_type, data_length, data_presicion in columns:
            if data_type.lower() in ["varchar2", "char", "nvarchar", "nchar", "varchar"]:
                table_structure.append(
                    (column_name.upper(), f"{data_type}({data_length})"))
            elif data_type.lower() in ["number"]:
                table_structure.append(
                    (column_name.upper(), f"{data_type}({data_presicion})"))
            else:
                table_structure.append((column_name.upper(), data_type))

        return table_structure
    except Exception as e:
        print(f"Error al obtener la estructura de la tabla: {str(e)}")
        return None

    finally:
        if cursor:
            cursor.close()


def get_data_query(sql_query, connection=target_conn):
    """
    Ejecuta una consulta SQL y devuelve los resultados.

    Parámetros:
    - sql_query (str): Consulta SQL que se desea ejecutar.
    - connection (obj, ): Conexión activa a la base de datos para ejecutar la consulta.

    Retorno:
    list: Lista de tuplas que contiene los datos obtenidos de la consulta. 
          Si ocurre un error, devuelve `None`.

    """

    cursor = connection.cursor()
    try:
        cursor.execute(sql_query)
        data = cursor.fetchall()
        return data
    except Exception as e:
        print(
            f"Error al obtener datos de la tabla {sql_query.split('FROM')[1].split(' ')[0]}: {str(e)}")
    finally:
        if cursor:
            cursor.close()


def execute_sql_query(sql_query, connection=target_conn):
    """
    Ejecuta una consulta SQL y confirma los cambios en la base de datos.

    Parámetros:
    - sql_query (str): Consulta SQL que se desea ejecutar.
    connection (obj, ): Conexión activa a la base de datos para ejecutar la consulta.

    Retorno:
    None: La función no devuelve ningún valor.
    """
    cursor = connection.cursor()
    try:
        cursor.execute(sql_query)
        connection.commit()
    except Exception as e:
        print(
            f"Error al ejecutar la consulta {' '.join(sql_query.split(' ')[0:4])}... : {str(e)}")
    finally:
        if cursor:
            cursor.close()


def create_sql_select(owner, table_name, columns):
    """
    Genera una consulta SQL SELECT para una tabla específica con expresiones personalizadas para las columnas.

    Parámetros:
    - owner (str): Esquema o propietario de la tabla.
    - table_name (str): Nombre de la tabla de la cual se seleccionarán los datos.
    - columns (list): Lista de tuplas `(column, expression)`, donde:
        - `column` es el nombre de la columna a mostrar en el resultado como alias.
        - `expression` es la expresión SQL que define los datos para esa columna.

    Retorno:
    str: Consulta SQL de tipo `SELECT`, con las columnas y alias especificados.


    """
    sql = f'SELECT {", ".join([f"{expression} AS [{column}]" for column, expression in columns])} FROM {owner}."{table_name.lower()}"'
    return sql


def create_sql_view(view_name, view_columns, data):
    """
    Genera una instrucción SQL para crear o modificar una vista con datos específicos.

    Parámetros:
    - view_name (str): Nombre de la vista que se creará o modificará.
    - view_columns (list): Lista de nombres de columnas a incluir en la vista.
    - data (str): Instrucción SQL o subconsulta que proporciona los datos de origen para la vista.

    Retorno:
    str: Instrucción SQL `CREATE OR ALTER VIEW` con la definición de la vista.
    """
    sql = f"""
CREATE OR ALTER VIEW {view_name} AS 
SELECT {", ".join([f"[{column}]" for column in view_columns])}
FROM (
{data}
) AS VISTA;
"""
    return sql


def get_table_data(owner, table_name, connection=oradbconn):
    """
    Obtiene todos los datos de una tabla específica en la base de datos.

    Parámetros:
    - owner (str): Esquema o propietario de la tabla en la base de datos.
    - table_name (str): Nombre de la tabla cuyos datos se desean obtener.
    - connection (obj, ): Conexión activa a la base de datos (por defecto usa `oradbconn`).

    Retorno:
    list: Lista de tuplas que contienen los datos de todas las filas de la tabla.
          Si ocurre un error, devuelve `None`.


    """
    cursor = connection.cursor()
    try:
        sql = f'SELECT * FROM {owner}."{table_name}"'
        cursor.execute(sql)
        data = cursor.fetchall()
        return data
    except Exception as e:
        print(f"Error al obtener datos de la base de datos: {str(e)}")
        return None
    finally:
        cursor.close()


def create_sql_insert(owner, table, numfields):
    """
    Genera una instrucción SQL INSERT para una tabla específica con un número dado de campos.

    Parámetros:
    - owner (str): Esquema o propietario de la tabla en la base de datos.
    - table (str): Nombre de la tabla en la que se desean insertar los datos.
    - numfields (int): Número de campos (columnas) que se van a insertar en la tabla.

    Retorno:
    str: Instrucción SQL `INSERT INTO` generada, con los valores de los campos representados por `?` como marcadores de posición.
    """
    sql = f'INSERT INTO {owner}."{table}" VALUES ('
    sql += ", ".join([f"?" for _ in range(numfields)]) + ")"
    return sql


def count_columns(table, db_type="oracle", connection=oradbconn):
    """
    Cuenta el número de columnas de una tabla en una base de datos específica.

    Parámetros:
    -  table (str): Nombre de la tabla cuyo número de columnas se desea contar.
    -  db_type (str, ): Tipo de base de datos, puede ser "oracle" o "sqlserver" (por defecto es "oracle").
    - connection (obj, ): Conexión activa a la base de datos para ejecutar la consulta (por defecto usa `oradbconn`).

    Retorno:
    int: Número de columnas en la tabla. Si ocurre un error, devuelve `None`.
    """
    cursor = connection.cursor()
    try:
        if db_type.lower() == "oracle":
            select_query = f"SELECT COUNT(*) FROM user_tab_columns WHERE table_name = '{table.upper()}'"
        elif db_type.lower() == "sqlserver":
            select_query = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table.upper()}'"
        cursor.execute(select_query)
        result = cursor.fetchone()[0]
        return result
    except Exception as e:
        print(f"Error al contar columnas: {str(e)}")
        return None
    finally:
        cursor.close()


def get_user_tables(db_type="oracle", connection=oradbconn):
    """
    Obtiene una lista de las tablas del usuario en una base de datos específica.

    Parámetros:
    db_type (str, ): Tipo de base de datos. Puede ser "oracle" o "sqlserver". 
                             El valor predeterminado es "oracle".
    connection (obj, ): Conexión activa a la base de datos para ejecutar la consulta 
                                (por defecto usa `oradbconn`).
    Retorno:
    list: Lista de nombres de tablas del usuario actual. Si ocurre un error, devuelve `None`.
    """

    cursor = connection.cursor()
    try:
        if db_type.lower() == "oracle":
            query = "SELECT table_name FROM user_tables"
        elif db_type.lower() == "sqlserver":
            query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
        cursor.execute(query)
        tables = [row[0] for row in cursor.fetchall()]
        return tables
    except Exception as e:
        print(f"Error al obtener las tablas de usuario: {str(e)}")
        return None
    finally:
        cursor.close()


def table_attributes(table_name, db_type="oracle", connection=oradbconn):
    """
    Obtiene el número de columnas y una lista de los nombres de columnas de una tabla específica.

    Parámetros:
    - table_name (str): Nombre de la tabla de la cual se desean obtener los atributos.
    - db_type (str, ): Tipo de base de datos. Puede ser "oracle" o "sqlserver". 
                             El valor predeterminado es "oracle".
    - connection (obj, ): Conexión activa a la base de datos para ejecutar la consulta 
                                (por defecto usa `oradbconn`).

    Retorno:
    tuple: Una tupla con dos elementos:
    - Un entero que representa el número de columnas de la tabla.
    - Una lista de cadenas que contiene los nombres de las columnas en orden.
    Si ocurre un error, se devuelve una tupla `(None, None)`.
    """

    cursor = connection.cursor()
    try:
        if db_type.lower() == "oracle":
            query = f"""
                SELECT COUNT(*), LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY column_id)
                FROM user_tab_columns
                WHERE table_name = '{table_name.upper()}'
            """
        elif db_type.lower() == "sqlserver":
            query = f"""
                SELECT COUNT(*), STRING_AGG(column_name, ', ') WITHIN GROUP (ORDER BY ordinal_position)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{table_name}'
            """
        cursor.execute(query)
        result = cursor.fetchone()
        return result[0], result[1].split(", ")
    except Exception as e:
        print(f"Error obteniendo atributos de la tabla {table_name}: {str(e)}")
        return None, None
    finally:
        cursor.close()


def statement_insert(table_name, table_attributes):
    """
    Genera una consulta SQL INSERT basada en los atributos de una tabla.

    Parámetros:
    - table_name (str): Nombre de la tabla en la cual se insertarán los datos.
    - table_attributes (tuple): Tupla que contiene dos elementos:
        - Un entero que representa el número de columnas de la tabla.
        - Una lista de nombres de las columnas en la tabla.

    Retorno:
    str: Una cadena que representa la consulta SQL `INSERT INTO` generada, con los nombres de las columnas
         y los marcadores de posición para los valores.
         Si los atributos de la tabla son inválidos (por ejemplo, si no se proporcionan columnas), devuelve `None`.
    """
    column_count, column_names = table_attributes
    if column_count and column_names:
        insert_query = f'INSERT INTO "{table_name}" ({", ".join(column_names)}) VALUES ({", ".join(["?" for _ in range(column_count)])})'
        return insert_query
    else:
        return None


def disable_restrictions(table_name, connection=target_conn3):
    """
    Deshabilita las restricciones de una tabla en una base de datos específica.

    Parámetros:
    - table_name (str): Nombre de la tabla en la que se desean deshabilitar las restricciones.
    - connection (obj, ): Conexión activa a la base de datos para ejecutar la consulta 
                                (por defecto usa `target_conn3`).
    """
    cursor = connection.cursor()
    try:
        query = f"ALTER TABLE {table_name} NOCHECK CONSTRAINT ALL"
        cursor.execute(query)
        connection.commit()
    except Exception as e:
        print(
            f"Error al desabilitar las retricciones de la tabla {table_name}: {str(e)}")
    finally:
        cursor.close()


def enable_restrictions(table_name, connection=target_conn3):
    """
    Habilita las restricciones de una tabla en una base de datos específica.

    Parámetros:
    - table_name (str): Nombre de la tabla en la que se desean habilitar las restricciones.
    - connection (obj, ): Conexión activa a la base de datos para ejecutar la consulta 
                                (por defecto usa `target_conn3`).
    Retorno:
    No devuelve ningún valor. Si ocurre un error, se imprime un mensaje de error.
    """
    cursor = connection.cursor()
    try:
        query = f"ALTER TABLE {table_name} WITH CHECK CHECK CONSTRAINT ALL"
        cursor.execute(query)
        connection.commit()
    except Exception as e:
        print(
            f"Error al habilitar las retricciones de la tabla {table_name}: {str(e)}")
    finally:
        cursor.close()

def generate_create_table_sql(table_json):
    """
    Genera una instrucción SQL para crear una tabla en SQL Server basada en una
    configuración JSON que describe la estructura de la tabla.

    Parámetros:
    - table_json (dict): JSON con los detalles de la tabla, incluyendo nombre,
      columnas y claves primarias.

    Retorna:
    str: una cadena con la instrucción SQL `CREATE TABLE`.
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