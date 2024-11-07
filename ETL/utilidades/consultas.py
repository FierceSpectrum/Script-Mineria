from conexiones.conexion_singleton import oradbconn, target_conn, target_conn2, target_conn3

def table_exists(table_name, owner, db_type="sqlserver", connection=target_conn):
    """
    Verifica si una tabla existe en una base de datos específica.

    Esta función comprueba si una tabla específica (`table_name`) existe en la base de datos
    bajo el esquema o propietario (`owner`) especificado. Es compatible con bases de datos 
    SQL Server y Oracle. Si la tabla existe, la función imprime un mensaje y devuelve `True`; 
    de lo contrario, imprime que no existe y devuelve `False`.

    Parámetros:
    table_name (str): Nombre de la tabla que se desea verificar.
    owner (str): Esquema o propietario de la tabla.
    db_type (str, opcional): Tipo de base de datos, que puede ser 'sqlserver' (por defecto) u 'oracle'.
    connection (obj, opcional): Conexión activa a la base de datos.

    Retorno:
    bool: Devuelve `True` si la tabla existe, y `False` si no.

    Excepciones:
    - ValueError: Si `db_type` no es 'sqlserver' ni 'oracle'.
    - Excepción general: Captura cualquier otro error de ejecución y lo muestra.

    Comportamiento:
    - Crea un cursor de base de datos usando `connection`.
    - Según el valor de `db_type`, define la consulta de verificación:
        - En Oracle, consulta en `all_tables` con el `owner` y `table_name` en mayúsculas.
        - En SQL Server, consulta en `INFORMATION_SCHEMA.TABLES` usando `TABLE_SCHEMA` y `TABLE_NAME`.
    - Ejecuta la consulta y verifica si existe la tabla.
    - Imprime y devuelve `True` si la tabla existe; de lo contrario, `False`.
    - Maneja excepciones de base de datos e imprime el error si ocurre.
    - Cierra el cursor al finalizar.

    Ejemplo:
    >>> table_exists("clientes", "public", db_type="sqlserver", connection=my_conn)
    La tabla clientes existe en public.
    """

    cursor = connection.cursor()
    try:
        if db_type.lower() == "oracle":
            check_query = f"SELECT COUNT(*) FROM all_tables WHERE owner = '{owner.upper()}' AND table_name = '{table_name.upper()}'"
        elif db_type.lower() == "sqlserver":
            check_query = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{owner}' AND TABLE_NAME = '{table_name}'"
        else:
            raise ValueError("Tipo de base de datos no soportado. Use 'oracle' o 'sqlserver'.")

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

    Esta función consulta la estructura de columnas de una tabla especificada (`table_name`) 
    en la base de datos para recuperar sus nombres de columna, tipos de datos y longitudes.
    Soporta bases de datos Oracle y SQL Server. Devuelve una lista con la estructura de la tabla 
    que incluye el nombre de cada columna y su tipo de dato (con tamaño o precisión cuando aplica).

    Parámetros:
    table_name (str): Nombre de la tabla cuyo esquema se quiere obtener.
    owner (str): Esquema o propietario de la tabla.
    db_type (str, opcional): Tipo de base de datos, que puede ser 'oracle' (por defecto) o 'sqlserver'.
    connection (obj, opcional): Conexión activa a la base de datos.

    Retorno:
    list: Lista de tuplas con la estructura de la tabla. Cada tupla contiene:
          (nombre_columna, tipo_dato) o (nombre_columna, tipo_dato(tamaño)) para datos de longitud variable.

    Excepciones:
    - ValueError: Si no se encuentran columnas para la tabla especificada.
    - Excepción general: Captura cualquier otro error de ejecución y lo muestra.

    Comportamiento:
    - Crea un cursor de base de datos usando `connection`.
    - Según el valor de `db_type`, define la consulta para obtener las columnas:
        - En Oracle, usa `all_tab_columns` con el `owner` y `table_name` en mayúsculas.
        - En SQL Server, usa `INFORMATION_SCHEMA.COLUMNS` con `TABLE_SCHEMA` y `TABLE_NAME`.
    - Ejecuta la consulta y obtiene todas las columnas y sus tipos de datos.
    - Si no se obtienen columnas, lanza una excepción `ValueError`.
    - Procesa cada columna según su tipo de dato:
        - Para tipos de longitud variable (`varchar2`, `char`, etc.), incluye la longitud.
        - Para `number`, incluye la precisión.
        - Para otros tipos, solo devuelve el nombre y tipo de dato.
    - Devuelve la estructura de la tabla como una lista de tuplas.

    Ejemplo:
    >>> get_table_structure("empleados", "HR", db_type="oracle", connection=oradbconn)
    [('ID', 'NUMBER(10)'), ('NOMBRE', 'VARCHAR2(50)'), ('FECHA_CONTRATACION', 'DATE')]
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

    Esta función toma una consulta SQL como entrada, la ejecuta usando la conexión a la base de datos
    proporcionada y devuelve todos los datos obtenidos por la consulta en forma de lista. Si ocurre
    algún error durante la ejecución, imprime un mensaje indicando el error y el nombre de la tabla 
    (obtenido a partir de la consulta SQL).

    Parámetros:
    sql_query (str): Consulta SQL que se desea ejecutar.
    connection (obj, opcional): Conexión activa a la base de datos para ejecutar la consulta.

    Retorno:
    list: Lista de tuplas que contiene los datos obtenidos de la consulta. 
          Si ocurre un error, devuelve `None`.

    Excepciones:
    - Excepción general: Captura cualquier error de ejecución, imprime el mensaje de error y el nombre de la tabla.

    Comportamiento:
    - Crea un cursor de base de datos usando `connection`.
    - Ejecuta la consulta SQL proporcionada.
    - Si la ejecución es exitosa, guarda todos los resultados en `data`.
    - Si ocurre un error, imprime el mensaje de error indicando la tabla afectada.
    - Cierra el cursor al finalizar la consulta.

    Ejemplo:
    >>> get_data_query("SELECT * FROM empleados", connection=my_conn)
    [(1, 'John', 'Doe'), (2, 'Jane', 'Smith'), ...]
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

    Esta función toma una consulta SQL como entrada y la ejecuta en la base de datos usando la
    conexión proporcionada. Después de ejecutar la consulta, realiza un `commit` para confirmar
    los cambios. Si ocurre algún error durante la ejecución, imprime un mensaje indicando el error 
    y una parte de la consulta SQL (los primeros cuatro elementos).

    Parámetros:
    sql_query (str): Consulta SQL que se desea ejecutar.
    connection (obj, opcional): Conexión activa a la base de datos para ejecutar la consulta.

    Retorno:
    None: La función no devuelve ningún valor.

    Excepciones:
    - Excepción general: Captura cualquier error de ejecución, imprime un mensaje de error y
      una parte de la consulta SQL.

    Comportamiento:
    - Crea un cursor de base de datos usando `connection`.
    - Ejecuta la consulta SQL proporcionada.
    - Realiza un `commit` en la conexión para aplicar los cambios.
    - Si ocurre un error, imprime el mensaje de error mostrando el tipo de error y los primeros 
      cuatro elementos de la consulta (por ejemplo, "UPDATE tabla SET ...").
    - Cierra el cursor al finalizar la consulta.

    Ejemplo:
    >>> execute_sql_query("UPDATE empleados SET salario = salario * 1.1", connection=my_conn)
    Actualiza el salario de todos los empleados en la tabla.
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

    Esta función toma un esquema (`owner`), un nombre de tabla (`table_name`) y una lista de columnas con expresiones,
    y genera una consulta SQL de selección (`SELECT`). Cada columna en la lista `columns` puede incluir un alias específico,
    permitiendo así seleccionar columnas calculadas o transformadas.

    Parámetros:
    owner (str): Esquema o propietario de la tabla.
    table_name (str): Nombre de la tabla de la cual se seleccionarán los datos.
    columns (list): Lista de tuplas `(column, expression)`, donde:
        - `column` es el nombre de la columna a mostrar en el resultado como alias.
        - `expression` es la expresión SQL que define los datos para esa columna.

    Retorno:
    str: Consulta SQL de tipo `SELECT`, con las columnas y alias especificados.

    Comportamiento:
    - Genera una consulta SQL `SELECT` con la lista de columnas en formato de alias (`AS`).
    - Usa `owner` para especificar el esquema de la tabla y `table_name.lower()` para el nombre de la tabla.
    - Cada columna en `columns` incluye la expresión SQL y un alias correspondiente.
    
    Ejemplo:
    >>> create_sql_select("HR", "empleados", [("nombre", "emp_name"), ("salario", "salary * 1.1")])
    'SELECT emp_name AS [nombre], salary * 1.1 AS [salario] FROM HR."empleados"'
    """
    sql = f'SELECT {", ".join([f"{expression} AS [{column}]" for column, expression in columns])} FROM {owner}."{table_name.lower()}"'
    return sql


def create_sql_view(view_name, view_columns, data):
    """
    Genera una instrucción SQL para crear o modificar una vista con datos específicos.

    Esta función genera un comando SQL para crear o modificar una vista (`VIEW`) con el nombre 
    especificado (`view_name`). La vista selecciona columnas específicas (`view_columns`) a partir 
    de una subconsulta o conjunto de datos proporcionado (`data`).

    Parámetros:
    view_name (str): Nombre de la vista que se creará o modificará.
    view_columns (list): Lista de nombres de columnas a incluir en la vista.
    data (str): Instrucción SQL o subconsulta que proporciona los datos de origen para la vista.

    Retorno:
    str: Instrucción SQL `CREATE OR ALTER VIEW` con la definición de la vista.

    Comportamiento:
    - Construye una instrucción `CREATE OR ALTER VIEW` con el nombre de la vista (`view_name`).
    - Usa `view_columns` para especificar las columnas de la vista.
    - Incluye `data` como una subconsulta para la vista, referenciándola como una tabla temporal (`AS VISTA`).
    
    Ejemplo:
    >>> create_sql_view("vista_empleados", ["nombre", "salario"], "SELECT emp_name AS nombre, salary AS salario FROM empleados")
    'CREATE OR ALTER VIEW vista_empleados AS \nSELECT [nombre], [salario]\nFROM (\nSELECT emp_name AS nombre, salary AS salario FROM empleados\n) AS VISTA;'
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

    Esta función ejecuta una consulta SQL para seleccionar todos los registros de una tabla especificada
    en el esquema indicado (`owner`). Luego, recupera todos los resultados de la consulta y los devuelve 
    como una lista de tuplas. Si ocurre un error durante la ejecución de la consulta, se captura la excepción 
    y se imprime un mensaje de error.

    Parámetros:
    owner (str): Esquema o propietario de la tabla en la base de datos.
    table_name (str): Nombre de la tabla cuyos datos se desean obtener.
    connection (obj, opcional): Conexión activa a la base de datos (por defecto usa `oradbconn`).

    Retorno:
    list: Lista de tuplas que contienen los datos de todas las filas de la tabla.
          Si ocurre un error, devuelve `None`.

    Excepciones:
    - Excepción general: Si ocurre un error al ejecutar la consulta, captura la excepción e imprime
      el mensaje de error.

    Comportamiento:
    - Crea un cursor de base de datos usando `connection`.
    - Construye y ejecuta una consulta SQL `SELECT *` para obtener todos los registros de la tabla.
    - Recupera los datos utilizando `fetchall()` y los devuelve como una lista de tuplas.
    - Si ocurre un error, imprime el mensaje de error.
    - Cierra el cursor después de completar la consulta.

    Ejemplo:
    >>> get_table_data("HR", "empleados", connection=my_conn)
    [(1, 'John', 'Doe', 3000), (2, 'Jane', 'Smith', 3200), ...]
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

    Esta función construye una consulta SQL de tipo `INSERT INTO` para insertar registros en una tabla
    del esquema especificado (`owner`). El número de valores a insertar es proporcionado por el parámetro 
    `numfields`, y se utiliza un marcador de posición (`?`) para cada campo en la consulta.

    Parámetros:
    owner (str): Esquema o propietario de la tabla en la base de datos.
    table (str): Nombre de la tabla en la que se desean insertar los datos.
    numfields (int): Número de campos (columnas) que se van a insertar en la tabla.

    Retorno:
    str: Instrucción SQL `INSERT INTO` generada, con los valores de los campos representados por `?` como marcadores de posición.

    Comportamiento:
    - Construye una consulta SQL `INSERT INTO` con el nombre de la tabla y el esquema.
    - Agrega el número adecuado de marcadores de posición `?` en la parte de valores de la consulta.
    - El valor de `numfields` se utiliza para determinar cuántos marcadores de posición se deben incluir.

    Ejemplo:
    >>> create_sql_insert("HR", "empleados", 3)
    'INSERT INTO HR."empleados" VALUES (?, ?, ?)'
    """
    sql = f'INSERT INTO {owner}."{table}" VALUES ('
    sql += ", ".join([f"?" for _ in range(numfields)]) + ")"
    return sql


def count_columns(table, db_type="oracle", connection=oradbconn):
    """
    Cuenta el número de columnas de una tabla en una base de datos específica.

    Esta función ejecuta una consulta SQL para contar el número de columnas en una tabla especificada
    dentro de una base de datos, ya sea de tipo Oracle o SQL Server. Dependiendo del tipo de base de datos,
    la consulta se adapta para obtener la información correcta sobre las columnas de la tabla indicada.

    Parámetros:
    table (str): Nombre de la tabla cuyo número de columnas se desea contar.
    db_type (str, opcional): Tipo de base de datos, puede ser "oracle" o "sqlserver" (por defecto es "oracle").
    connection (obj, opcional): Conexión activa a la base de datos para ejecutar la consulta (por defecto usa `oradbconn`).

    Retorno:
    int: Número de columnas en la tabla. Si ocurre un error, devuelve `None`.

    Excepciones:
    - Excepción general: Si ocurre un error al ejecutar la consulta, captura la excepción e imprime el mensaje de error.

    Comportamiento:
    - Dependiendo de `db_type`, ejecuta una consulta SQL para contar las columnas de la tabla en Oracle o SQL Server.
    - Si la consulta es exitosa, devuelve el número de columnas.
    - Si ocurre un error, imprime el mensaje de error.
    - Cierra el cursor después de completar la consulta.

    Ejemplo:
    >>> count_columns("empleados", db_type="oracle", connection=my_conn)
    5
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

    Esta función ejecuta una consulta SQL para obtener los nombres de todas las tablas 
    del usuario actual en una base de datos, adaptando la consulta según el tipo de base 
    de datos (`db_type`). Soporta bases de datos Oracle y SQL Server. Si ocurre un error 
    durante la ejecución, se captura la excepción y se imprime un mensaje de error.

    Parámetros:
    db_type (str, opcional): Tipo de base de datos. Puede ser "oracle" o "sqlserver". 
                             El valor predeterminado es "oracle".
    connection (obj, opcional): Conexión activa a la base de datos para ejecutar la consulta 
                                (por defecto usa `oradbconn`).

    Retorno:
    list: Lista de nombres de tablas del usuario actual. Si ocurre un error, devuelve `None`.

    Excepciones:
    - Excepción general: Si ocurre un error al ejecutar la consulta, captura la excepción e imprime
      el mensaje de error.

    Comportamiento:
    - Dependiendo de `db_type`, ejecuta una consulta SQL para obtener los nombres de las tablas del usuario
      en Oracle o SQL Server.
    - Si la consulta es exitosa, devuelve una lista con los nombres de las tablas.
    - Si ocurre un error, imprime el mensaje de error.
    - Cierra el cursor después de completar la consulta.

    Ejemplo:
    >>> get_user_tables("oracle", connection=my_conn)
    ['empleados', 'clientes', 'productos', ...]
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

    Esta función ejecuta una consulta SQL para obtener dos atributos de una tabla especificada:
    1. El número de columnas que tiene la tabla.
    2. Una lista de los nombres de las columnas ordenadas según su posición en la tabla.

    Dependiendo del tipo de base de datos (`db_type`), la consulta se adapta para obtener la información
    correspondiente en Oracle o SQL Server.

    Parámetros:
    table_name (str): Nombre de la tabla de la cual se desean obtener los atributos.
    db_type (str, opcional): Tipo de base de datos. Puede ser "oracle" o "sqlserver". 
                             El valor predeterminado es "oracle".
    connection (obj, opcional): Conexión activa a la base de datos para ejecutar la consulta 
                                (por defecto usa `oradbconn`).

    Retorno:
    tuple: Una tupla con dos elementos:
        - Un entero que representa el número de columnas de la tabla.
        - Una lista de cadenas que contiene los nombres de las columnas en orden.
        Si ocurre un error, se devuelve una tupla `(None, None)`.

    Excepciones:
    - Excepción general: Si ocurre un error al ejecutar la consulta, captura la excepción e imprime
      el mensaje de error.

    Comportamiento:
    - Dependiendo de `db_type`, ejecuta una consulta SQL para obtener el número de columnas y sus nombres
      en Oracle o SQL Server.
    - Si la consulta es exitosa, devuelve el número de columnas y la lista de nombres de las columnas.
    - Si ocurre un error, imprime el mensaje de error y devuelve `(None, None)`.
    - Cierra el cursor después de completar la consulta.

    Ejemplo:
    >>> table_attributes("empleados", db_type="oracle", connection=my_conn)
    (5, ['ID_EMPLEADO', 'NOMBRE', 'APELLIDO', 'SALARIO', 'FECHA_INGRESO'])
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

    Esta función construye una consulta SQL de tipo `INSERT INTO` para una tabla específica. La consulta se 
    genera utilizando los nombres de las columnas y la cantidad de columnas obtenidas de los atributos de la tabla
    proporcionados como parámetro. Los valores para cada columna se representan mediante marcadores de posición (`?`).

    Parámetros:
    table_name (str): Nombre de la tabla en la cual se insertarán los datos.
    table_attributes (tuple): Tupla que contiene dos elementos:
        - Un entero que representa el número de columnas de la tabla.
        - Una lista de nombres de las columnas en la tabla.

    Retorno:
    str: Una cadena que representa la consulta SQL `INSERT INTO` generada, con los nombres de las columnas
         y los marcadores de posición para los valores.
         Si los atributos de la tabla son inválidos (por ejemplo, si no se proporcionan columnas), devuelve `None`.

    Comportamiento:
    - Utiliza los atributos de la tabla (`table_attributes`) para obtener el número de columnas y sus nombres.
    - Si los atributos de la tabla son válidos, construye una consulta SQL `INSERT INTO` con la sintaxis adecuada.
    - Si los atributos no son válidos (por ejemplo, si la tabla no tiene columnas o no se pasan correctamente),
      devuelve `None`.

    Excepciones:
    - No se capturan excepciones explícitamente en la función, pero si los parámetros no son válidos, 
      se retornará `None`.

    Ejemplo:
    >>> statement_insert("empleados", (5, ['ID_EMPLEADO', 'NOMBRE', 'APELLIDO', 'SALARIO', 'FECHA_INGRESO']))
    'INSERT INTO "empleados" (ID_EMPLEADO, NOMBRE, APELLIDO, SALARIO, FECHA_INGRESO) VALUES (?, ?, ?, ?, ?)'
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

    Esta función ejecuta una consulta SQL para deshabilitar todas las restricciones de una tabla, 
    utilizando el comando `ALTER TABLE ... NOCHECK CONSTRAINT ALL`. Es útil cuando se necesita 
    deshabilitar temporalmente las restricciones (como claves foráneas) para realizar operaciones 
    de manipulación de datos en la tabla.

    Parámetros:
    table_name (str): Nombre de la tabla en la que se desean deshabilitar las restricciones.
    connection (obj, opcional): Conexión activa a la base de datos para ejecutar la consulta 
                                (por defecto usa `target_conn3`).

    Comportamiento:
    - La función ejecuta una consulta SQL para deshabilitar las restricciones de la tabla indicada.
    - Si la consulta se ejecuta con éxito, se confirma la transacción.
    - Si ocurre un error al ejecutar la consulta, se captura la excepción y se imprime un mensaje de error.

    Excepciones:
    - Excepción general: Si ocurre un error al ejecutar la consulta, captura la excepción e imprime
      el mensaje de error.

    Retorno:
    - No devuelve ningún valor. Si ocurre un error, se imprime un mensaje de error.

    Ejemplo:
    >>> disable_restrictions("empleados", connection=my_conn)
    # Deshabilita las restricciones en la tabla "empleados"
    """
    cursor = connection.cursor()
    try:
        query= f"ALTER TABLE {table_name} NOCHECK CONSTRAINT ALL"
        cursor.execute(query)
        connection.commit()
    except Exception as e:
        print(f"Error al desabilitar las retricciones de la tabla {table_name}: {str(e)}")
    finally:
        cursor.close()

def enable_restrictions(table_name, connection=target_conn3):
    """
    Habilita las restricciones de una tabla en una base de datos específica.

    Esta función ejecuta una consulta SQL para habilitar todas las restricciones de una tabla, 
    utilizando el comando `ALTER TABLE ... WITH CHECK CHECK CONSTRAINT ALL`. Es útil para restaurar
    las restricciones (como claves foráneas) después de haber sido deshabilitadas, asegurando que
    las restricciones se apliquen y se verifiquen.

    Parámetros:
    table_name (str): Nombre de la tabla en la que se desean habilitar las restricciones.
    connection (obj, opcional): Conexión activa a la base de datos para ejecutar la consulta 
                                (por defecto usa `target_conn3`).

    Comportamiento:
    - La función ejecuta una consulta SQL para habilitar las restricciones de la tabla indicada.
    - Si la consulta se ejecuta con éxito, se confirma la transacción.
    - Si ocurre un error al ejecutar la consulta, se captura la excepción y se imprime un mensaje de error.

    Excepciones:
    - Excepción general: Si ocurre un error al ejecutar la consulta, captura la excepción e imprime
      el mensaje de error.

    Retorno:
    - No devuelve ningún valor. Si ocurre un error, se imprime un mensaje de error.

    Ejemplo:
    >>> enable_restrictions("empleados", connection=my_conn)
    # Habilita las restricciones en la tabla "empleados"
    """
    cursor = connection.cursor()
    try:
        query= f"ALTER TABLE {table_name} WITH CHECK CHECK CONSTRAINT ALL"
        cursor.execute(query)
        connection.commit()
    except Exception as e:
        print(f"Error al habilitar las retricciones de la tabla {table_name}: {str(e)}")
    finally:
        cursor.close()