"""
Módulo de Administración de Datos en SQL Server y Oracle

Este módulo proporciona funciones para insertar, eliminar, y administrar datos en tablas de SQL Server
y Oracle, así como para crear y actualizar vistas en la base de datos. Estas operaciones permiten manejar
de manera eficiente la estructura de datos y contenido en entornos de bases de datos relacionales.

Funcionalidades principales:
- `add_data_entity(data, tablename, numfields, connection)`: Inserta datos en una tabla de SQL Server.
- `delete_data_entity(table, operation, connection)`: Elimina datos de una tabla usando `DELETE` o `TRUNCATE`.
- `execute_oracle_procedure(pprocedure)`: Ejecuta un procedimiento almacenado en Oracle.
- `create_table(table_name, owner, table_structure, db_type, connection, autoincrementalid)`: Crea una tabla
   en la base de datos con la estructura especificada, compatible con SQL Server y Oracle.
- `execute_sql_view(sql_query, connection)`: Ejecuta una consulta SQL para crear o modificar una vista en la base de datos.

Uso:
Este módulo está diseñado para integrarse en procesos ETL o en tareas de administración de bases de datos.
Permite la manipulación de datos y la creación de estructuras de tablas y vistas, facilitando el manejo de datos
en entornos de bases de datos relacionales.

Requerimientos:
- `conexiones.conexion_singleton`: Para establecer conexiones a las bases de datos SQL Server y Oracle.
- Módulo `utilidades.formato` para la conversión de tipos de datos entre Oracle y SQL Server.
"""

from utilidades import frt, cst
from conexiones import oradbconn, target_conn, target_conn2, target_conn3


def add_data_entity(data, tablename, numfields, connection=target_conn):
    """
    Inserta datos en una tabla de SQL Server.

    Parámetros:
    - data (list of tuples): Los datos a insertar en la tabla. Cada tupla representa una fila de datos.
    - tablename (str): El nombre de la tabla en la que se insertarán los datos.
    - numfields (int): El número de columnas en la tabla a insertar, que determina la cantidad de valores
                     en cada fila de datos.
    - connection (optional, default=target_conn): La conexión a la base de datos de SQL Server. Si no se especifica,
                                                se usará la conexión predeterminada `target_conn`.

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

    Parámetros:
    - table (str): El nombre de la tabla de la cual se eliminarán los datos.
    - operation (str): La operación que se va a realizar, puede ser `'DELETE'` o `'TRUNCATE'`.
    - connection (object): La conexión a la base de datos. Por defecto, usa `target_conn`.
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

    Parámetros:
    - pprocedure (str): El nombre del procedimiento almacenado que se desea ejecutar.
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

    Parámetros:
    - table_name (str): El nombre de la tabla que se desea crear.
    - owner (str): El propietario o esquema donde se creará la tabla.
    - table_structure (list): Una lista de tuplas que especifican los nombres de las columnas y sus tipos de datos.
                            Ejemplo: [('columna1', 'VARCHAR(100)'), ('columna2', 'INT')]
    - db_type (str): El tipo de base de datos en la que se creará la tabla. Los valores posibles son 'sqlserver' (por defecto)
                             o 'oracle'. Este parámetro determina el tipo de datos adecuado para las columnas.
    - connection (objeto de conexión): Objeto de conexión a la base de datos. Por defecto, se usa `target_conn`.
    - autoincrementalid (bool): Si se establece en `True`, se agrega una columna autoincremental llamada 
                                         "{nombre_tabla}_KEY" como clave primaria.
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

    Parámetros:
    - sql_query (str): Consulta SQL para crear o modificar una vista. Debe ser una cadena que contenga una 
                     instrucción SQL válida, por ejemplo, "CREATE OR ALTER VIEW vista_ejemplo AS SELECT ...".
    - connection (objeto de conexión): Conexión a la base de datos. Se utiliza `target_conn` por defecto.
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
