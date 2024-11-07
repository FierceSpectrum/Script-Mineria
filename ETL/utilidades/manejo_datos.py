from utilidades import formato as frt, consultas as cst
from conexiones.conexion_singleton import oradbconn, target_conn, target_conn2, target_conn3

def add_data_entity(data, tablename, numfields, connection=target_conn):
    """
    Inserta datos en una tabla de SQL Server.

    Esta función toma una lista de datos y los inserta en una tabla de SQL Server. La inserción se realiza
    mediante la ejecución de una consulta `INSERT INTO` generada con la función `create_sql_insert`. 

    Parámetros:
    data (list of tuples): Los datos a insertar en la tabla. Cada tupla representa una fila de datos.
    tablename (str): El nombre de la tabla en la que se insertarán los datos.
    numfields (int): El número de columnas en la tabla a insertar, que determina la cantidad de valores
                     en cada fila de datos.
    connection (optional, default=target_conn): La conexión a la base de datos de SQL Server. Si no se especifica,
                                                se usará la conexión predeterminada `target_conn`.

    Comportamiento:
    - La función utiliza la consulta `INSERT INTO` generada por la función `create_sql_insert` para insertar
      los datos en la tabla especificada.
    - Si los datos son insertados correctamente, se realiza un commit en la base de datos.
    - Si ocurre un error durante la inserción, la transacción se revierte y se imprime un mensaje de error.

    Excepciones:
    - Si hay un error durante la inserción (por ejemplo, si hay un error de conexión o la estructura de los datos
      no coincide con la tabla), se captura la excepción y se imprime un mensaje de error.

    Ejemplo:
    >>> data = [(1, 'John Doe', 29), (2, 'Jane Smith', 34)]
    >>> add_data_entity(data, 'employees', 3)
    >>> # Inserta los datos de la lista en la tabla 'employees'.
    """
    sql = cst.create_sql_insert("dbo", tablename, numfields)

    if data:
        cursor = connection.cursor()
        try:
            cursor.executemany(sql, data)
            connection.commit()
            print(f"Datos insertados en la tabla {tablename} de SQL Server.")
        except Exception as e:
            print(f"Error al insertar datos en la tabla {tablename}: {str(e)}")
            target_conn.rollback()  # Revierte en caso de error
        finally:
            cursor.close()


def delete_data_entity(table, operation, connection=target_conn):
    """
    Elimina todos los datos de una tabla utilizando una operación SQL específica.

    Esta función permite eliminar datos de una tabla usando dos operaciones: 
    `DELETE` o `TRUNCATE`. Dependiendo de la operación seleccionada, se ejecutará 
    la correspondiente consulta SQL para borrar los datos de la tabla en la base de datos.

    Parámetros:
    table (str): El nombre de la tabla de la cual se eliminarán los datos.
    operation (str): La operación que se va a realizar, puede ser `'DELETE'` o `'TRUNCATE'`.
    connection (object, opcional): La conexión a la base de datos. Por defecto, usa `target_conn`.

    Retorno:
    None

    Comportamiento:
    - Si la operación es `DELETE`, eliminará todas las filas de la tabla, pero mantendrá la estructura de la tabla.
    - Si la operación es `TRUNCATE`, eliminará todas las filas de la tabla de forma más eficiente y rápida que `DELETE`.
    - Si la operación no es válida, se generará una excepción.
    - La transacción se confirma con un `commit` después de la operación.
    - Si ocurre un error, se captura la excepción, se imprime un mensaje de error y la transacción no se realiza.

    Excepciones:
    - Si la operación no es `'DELETE'` ni `'TRUNCATE'`, se genera una excepción `ValueError`.

    Ejemplo:
    >>> delete_data_entity("my_table", "DELETE")
    >>> # Elimina todas las filas de la tabla "my_table" utilizando DELETE.

    >>> delete_data_entity("my_table", "TRUNCATE")
    >>> # Elimina todas las filas de la tabla "my_table" utilizando TRUNCATE.
    """
    cursor = connection.cursor()
    try:
        if operation.upper() == 'DELETE':
            delete_query = f'DELETE FROM "{table}"'
            cursor.execute(delete_query)
        elif operation.upper() == 'TRUNCATE':
            truncate_query = f"TRUNCATE TABLE [{table}]"
            cursor.execute(truncate_query)
        else:
            raise ValueError("Operación no válida. Use 'DELETE' o 'TRUNCATE'.")

        connection.commit()
        print(f"Operación {operation} realizada en la tabla {table}.")
    except Exception as e:
        print(f"Error al realizar la operación: {str(e)}")
    finally:
        cursor.close()


def execute_oracle_procedure(pprocedure):
    """
    Ejecuta un procedimiento almacenado en una base de datos Oracle.

    Esta función permite ejecutar procedimientos almacenados en la base de datos Oracle
    especificando el nombre del procedimiento a ejecutar. Si el procedimiento realiza 
    modificaciones en los datos, los cambios son confirmados con un `commit`.

    Parámetros:
    pprocedure (str): El nombre del procedimiento almacenado que se desea ejecutar.

    Retorno:
    None

    Comportamiento:
    - Ejecuta el procedimiento almacenado especificado utilizando el método `callproc`.
    - Si el procedimiento realiza cambios en la base de datos, se confirma la transacción con un `commit`.
    - Si ocurre un error durante la ejecución del procedimiento, se captura la excepción y se imprime un mensaje de error.

    Excepciones:
    - Si ocurre un error durante la ejecución del procedimiento almacenado, la excepción es capturada e informada.

    Ejemplo:
    >>> execute_oracle_procedure("mi_procedimiento")
    >>> # Ejecuta el procedimiento "mi_procedimiento" en la base de datos Oracle.

    >>> execute_oracle_procedure("procedimiento_con_parametros")
    >>> # Ejecuta el procedimiento "procedimiento_con_parametros" en Oracle.
    """
    cursor = oradbconn.cursor()
    try:
        cursor.callproc(pprocedure)
        oradbconn.commit()  # Confirma los cambios si el procedimiento realiza modificaciones
    except Exception as e:
        print(f"Error al ejecutar el procedimiento {pprocedure}: {str(e)}")
    finally:
        cursor.close()


def create_table(table_name, owner, table_structure, db_type="sqlserver", connection=target_conn, autoincrementalid=False):
    """
    Crea una tabla en la base de datos especificada con la estructura indicada.

    Esta función genera una consulta SQL para crear una tabla en la base de datos, de acuerdo con
    el tipo de base de datos proporcionado (por defecto, SQL Server). También puede incluir una columna
    de clave primaria autoincremental si se especifica el parámetro `autoincrementalid`.

    Parámetros:
    table_name (str): El nombre de la tabla que se desea crear.
    owner (str): El propietario o esquema donde se creará la tabla.
    table_structure (list): Una lista de tuplas que especifican los nombres de las columnas y sus tipos de datos.
                            Ejemplo: [('columna1', 'VARCHAR(100)'), ('columna2', 'INT')]
    db_type (str, opcional): El tipo de base de datos en la que se creará la tabla. Los valores posibles son 'sqlserver' (por defecto)
                             o 'oracle'. Este parámetro determina el tipo de datos adecuado para las columnas.
    connection (objeto de conexión, opcional): Objeto de conexión a la base de datos. Por defecto, se usa `target_conn`.
    autoincrementalid (bool, opcional): Si se establece en `True`, se agrega una columna autoincremental llamada 
                                         "{nombre_tabla}_KEY" como clave primaria.

    Retorno:
    None

    Comportamiento:
    - Construye una consulta SQL `CREATE TABLE` usando la estructura de las columnas proporcionada.
    - Si `autoincrementalid` es `True`, agrega una columna autoincremental como clave primaria.
    - Ejecuta la consulta SQL y crea la tabla en la base de datos.
    - Si se produce un error durante la creación, imprime un mensaje de error detallado.
    - Si la tabla se crea correctamente, imprime un mensaje confirmando la creación.

    Excepciones:
    - Si ocurre un error al ejecutar la consulta (por ejemplo, debido a un error de sintaxis o problemas con la conexión), 
      se captura la excepción y se imprime un mensaje detallado con el error.

    Ejemplo:
    >>> create_table("empleados", "hr", [("id", "INT"), ("nombre", "VARCHAR(100)")], db_type="sqlserver")
    >>> # Crea la tabla "empleados" en el esquema "hr" con las columnas "id" y "nombre" en SQL Server.

    >>> create_table("productos", "ventas", [("id", "INT"), ("descripcion", "VARCHAR(255)")], autoincrementalid=True)
    >>> # Crea la tabla "productos" en el esquema "ventas" con una columna autoincremental "id_KEY" en SQL Server.
    """
    cursor = connection.cursor()
    try:
        # Contruir la consulta CREATE TABLE
        create_query = f'CREATE TABLE {owner.upper()}."{table_name.upper()}" ('

        column_definitions = []
        for column_name, data_type in table_structure:
            converted_type = frt.convert_data_type(data_type, db_type)
            column_definitions.append(f"{column_name} {converted_type}")

        if autoincrementalid:
            name = table_name.split("_")[1]
            name = name[:len(name)-1]
            column_definitions.insert(
                0, f"{name}_KEY INT IDENTITY(1,1) CONSTRAINT PK_{name} PRIMARY KEY")

        create_query += ", ".join(column_definitions) + ")"
        cursor.execute(create_query)
        connection.commit()

        print(f"Table {table_name} creada exitosamente en {owner}.")

    except Exception as e:
        print(f"Error al crear la tabla: {str(e)}")

    finally:
        if cursor:
            cursor.close()


def execute_sql_view(sql_query, connection=target_conn):
    """
    Ejecuta una consulta SQL para crear o modificar una vista en la base de datos.

    Esta función toma una consulta SQL en forma de cadena que debe contener una definición de vista,
    ejecuta esa consulta en la base de datos y confirma la transacción si la consulta es exitosa.

    Parámetros:
    sql_query (str): Consulta SQL para crear o modificar una vista. Debe ser una cadena que contenga una 
                     instrucción SQL válida, por ejemplo, "CREATE OR ALTER VIEW vista_ejemplo AS SELECT ...".
    connection (objeto de conexión, opcional): Conexión a la base de datos. Se utiliza `target_conn` por defecto.

    Retorno:
    None

    Comportamiento:
    - Extrae el nombre de la vista de la consulta SQL proporcionada.
    - Ejecuta la consulta SQL para crear o modificar la vista en la base de datos.
    - Si la consulta es exitosa, realiza un `commit` para confirmar los cambios y muestra un mensaje 
      indicando que la vista fue creada correctamente.
    - Si ocurre un error durante la ejecución de la consulta, captura la excepción y muestra un mensaje de error.
    - Siempre cierra el cursor después de ejecutar la consulta, incluso si ocurrió un error.

    Excepciones:
    - Si ocurre un error durante la ejecución de la consulta (por ejemplo, un error de sintaxis SQL o 
      problemas con la conexión), se captura la excepción y se imprime un mensaje detallado con el error.

    Ejemplo:
    >>> sql_query = "CREATE OR ALTER VIEW vista_ejemplo AS SELECT columna1, columna2 FROM tabla_ejemplo"
    >>> execute_sql_view(sql_query)
    >>> # Crea o altera la vista "vista_ejemplo" en la base de datos.

    >>> sql_query = "CREATE VIEW nueva_vista AS SELECT * FROM otra_tabla"
    >>> execute_sql_view(sql_query)
    >>> # Crea la vista "nueva_vista" en la base de datos.
    """
    cursor = connection.cursor()
    try:
        view_name = sql_query.split(" ")[4]
        cursor.execute(sql_query)
        connection.commit()
        print(f"Vista {view_name} creada exitosamente.")
    except Exception as e:
        print(
            f"Error al crear la vista {view_name}: {str(e)}")
    finally:
        if cursor:
            cursor.close()
