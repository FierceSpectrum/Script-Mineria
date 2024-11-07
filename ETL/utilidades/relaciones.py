from conexiones.conexion_singleton import oradbconn, target_conn, target_conn2, target_conn3


def create_reference_sql(table_name, column, table_reference, column_reference):
    """
    Genera una consulta SQL para crear una restricción de clave foránea (FOREIGN KEY) entre dos tablas.

    Esta función construye una consulta SQL para crear una relación de clave foránea entre una columna
    de la tabla `table_name` y una columna de referencia `column_reference` en la tabla `table_reference`.

    Parámetros:
    table_name (str): Nombre de la tabla que contiene la columna que se va a referenciar.
    column (str): Nombre de la columna en la tabla `table_name` que se va a utilizar como clave foránea.
    table_reference (str): Nombre de la tabla a la que se va a hacer referencia.
    column_reference (str): Nombre de la columna en la tabla de referencia que será referenciada.

    Retorno:
    str: La consulta SQL generada como una cadena de texto para crear la restricción de clave foránea.

    Comportamiento:
    - La función genera una consulta SQL en formato de cadena para crear una restricción de clave foránea entre 
      las tablas proporcionadas.
    - El nombre de la restricción de la clave foránea se genera de manera automática, combinando los nombres de 
      las tablas involucradas en la relación.
    - La consulta es impresa en la consola y también se retorna como una cadena para su posterior uso.

    Ejemplo:
    >>> create_reference_sql('orders', 'customer_id', 'customers', 'id')
    ALTER TABLE orders ADD CONSTRAINT FK_orders_customers FOREIGN KEY (customer_id) REFERENCES customers(id)
    >>> # Retorna la consulta SQL para crear una clave foránea entre las tablas 'orders' y 'customers'.

    >>> create_reference_sql('employees', 'department_id', 'departments', 'id')
    ALTER TABLE employees ADD CONSTRAINT FK_employees_departments FOREIGN KEY (department_id) REFERENCES departments(id)
    >>> # Retorna la consulta SQL para crear una clave foránea entre las tablas 'employees' y 'departments'.
    """
    name_restriction = f"FK_{table_name.split('_')[1]}_{table_reference.split('_')[1]}"
    script = f"ALTER TABLE {table_name} ADD CONSTRAINT {name_restriction} foreign key ({column}) references {table_reference}({column_reference})"
    print(script)
    return script


def has_foreign_key_relationship(parent_table, referenced_table, db_type="sqlserver", connection=target_conn3):
    """
    Verifica si existe una relación de llave foránea entre dos tablas.

    Parámetros:
    - parent_table: el nombre de la tabla que contiene la llave foránea.
    - referenced_table: el nombre de la tabla referenciada.
    - db_type: el tipo de base de datos ('sqlserver' u 'oracle').
    - connection: la conexión a la base de datos.

    Retorna:
    - True si existe una llave foránea entre las tablas, False en caso contrario.
    """
    cursor = connection.cursor()

    if db_type.lower() == 'sqlserver':
        query = f"""
        SELECT 
            CASE 
                WHEN EXISTS (
                    SELECT 1
                    FROM 
                        sys.foreign_keys AS fk
                    JOIN 
                        sys.tables AS parent ON fk.parent_object_id = parent.object_id
                    JOIN 
                        sys.tables AS referenced ON fk.referenced_object_id = referenced.object_id
                    WHERE 
                        parent.name = '{parent_table}'
                        AND referenced.name = '{referenced_table}'
                ) THEN 1
                ELSE 0
            END AS HasForeignKey;
        """

    elif db_type.lower() == 'oracle':
        query = f"""
        SELECT 
            CASE 
                WHEN EXISTS (
                    SELECT 1
                    FROM 
                        all_constraints a
                    JOIN 
                        all_cons_columns c ON a.constraint_name = c.constraint_name
                    JOIN 
                        all_constraints r ON a.r_constraint_name = r.constraint_name
                    WHERE 
                        a.constraint_type = 'R'
                        AND a.table_name = '{parent_table}'
                        AND r.table_name = '{referenced_table}'
                ) THEN 1
                ELSE 0
            END AS HasForeignKey
        FROM dual;
        """

    else:
        raise ValueError(
            "Tipo de base de datos no soportado. Usa 'sqlserver' u 'oracle'.")
    try:
        cursor.execute(query)
        result = cursor.fetchone()
        return bool(result[0])
    except Exception as e:
        print(f"Error al buscar la realacion entre las tablas: {str(e)}")
        return None
    finally:
        cursor.close()


def has_realtionship(table_name, connection=target_conn3):
    """
    Verifica si existe una relación de llave foránea de una tabla.

    Parámetros:
    - table_name: el nombre de la tabla que busca la llave foránea.
    - connection: la conexión a la base de datos.

    Retorna:
    - True si existe una llave foránea, False en caso contrario.
    """
    cursor = connection.cursor()

    query = f"""
    SELECT 
        fk.name AS ForeignKeyName,
        parent.name AS ParentTable,
        parent_col.name AS ParentColumn,
        referenced.name AS ReferencedTable,
        referenced_col.name AS ReferencedColumn
    FROM 
        sys.foreign_keys AS fk
    JOIN 
        sys.foreign_key_columns AS fkc ON fk.object_id = fkc.constraint_object_id
    JOIN 
        sys.tables AS parent ON fkc.parent_object_id = parent.object_id
    JOIN 
        sys.columns AS parent_col ON fkc.parent_object_id = parent_col.object_id 
        AND fkc.parent_column_id = parent_col.column_id
    JOIN 
        sys.tables AS referenced ON fkc.referenced_object_id = referenced.object_id
    JOIN 
        sys.columns AS referenced_col ON fkc.referenced_object_id = referenced_col.object_id 
        AND fkc.referenced_column_id = referenced_col.column_id
    WHERE 
        referenced.name = '{table_name}';
    """
    try:
        cursor.execute(query)
        result = cursor.fetchone()
        if result:
            return (bool(result[0]), result[1])
        return None, None

    except Exception as e:
        print(f"Error al buscar realaciones de la tabla: {str(e)}")
        return None, None
    finally:
        cursor.close()


def chage_data_for_id(data, positition, ids):
    """
    Modifica los valores de una columna específica en los datos de entrada basándose en un diccionario de IDs.

    Esta función toma los datos de una tabla o lista de registros y cambia los valores en una columna específica 
    (denominada `positition`) utilizando un diccionario `ids` que mapea valores antiguos de esa columna a nuevos valores.

    Parámetros:
    data (list): Lista de registros, donde cada registro es una lista de valores (simulando filas de una tabla).
    positition (int): Índice de la columna en la que se van a realizar los cambios.
    ids (dict): Diccionario que mapea los valores antiguos de la columna a nuevos valores. La clave es el valor antiguo 
                y el valor es el valor nuevo al que se debe cambiar.

    Retorno:
    list: La lista de datos modificada con los nuevos valores en la columna especificada.

    Excepciones:
    Si ocurre un error durante el proceso de modificación, se captura la excepción y se imprime un mensaje de error.

    Comportamiento:
    La función recorre los registros de los datos y reemplaza los valores en la columna indicada por los valores 
    correspondientes en el diccionario `ids`. La clave del diccionario debe coincidir con el valor original en la columna 
    que se va a modificar.

    Ejemplo:
    >>> data = [[1, "Alice", 101], [2, "Bob", 102], [3, "Charlie", 103]]
    >>> ids = {101: 201, 102: 202, 103: 203}
    >>> chage_data_for_id(data, 2, ids)
    [[1, "Alice", 201], [2, "Bob", 202], [3, "Charlie", 203]]
    >>> # Los valores de la columna 2 (índice 2) han sido modificados utilizando el diccionario `ids`.
    """
    position_db = len(data[0]) - 1

    try:
        for file in data:
            file[positition] = ids[file[position_db]][file[positition]]
        return data
    except Exception as e:
        print(f"Error al cambiar datos: {str(e)}")
