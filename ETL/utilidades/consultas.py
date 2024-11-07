from conexiones.conexion_singleton import oradbconn, target_conn, target_conn2, target_conn3

def table_exists(table_name, owner, db_type="sqlserver", connection=target_conn):
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
    sql = f'SELECT {", ".join([f"{expression} AS [{column}]" for column, expression in columns])} FROM {owner}."{table_name.lower()}"'
    return sql


def create_sql_view(view_name, view_columns, data):
    sql = f"""
CREATE OR ALTER VIEW {view_name} AS 
SELECT {", ".join([f"[{column}]" for column in view_columns])}
FROM (
{data}
) AS VISTA;
"""
    return sql


def get_table_data(owner, table_name, connection=oradbconn):
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
    sql = f'INSERT INTO {owner}."{table}" VALUES ('
    sql += ", ".join([f"?" for _ in range(numfields)]) + ")"
    return sql


def count_columns(table, db_type="oracle", connection=oradbconn):
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
    column_count, column_names = table_attributes
    if column_count and column_names:
        insert_query = f'INSERT INTO "{table_name}" ({", ".join(column_names)}) VALUES ({", ".join(["?" for _ in range(column_count)])})'
        return insert_query
    else:
        return None

def disable_restrictions(table_name, connection=target_conn3):
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
    cursor = connection.cursor()
    try:
        query= f"ALTER TABLE {table_name} WITH CHECK CHECK CONSTRAINT ALL"
        cursor.execute(query)
        connection.commit()
    except Exception as e:
        print(f"Error al habilitar las retricciones de la tabla {table_name}: {str(e)}")
    finally:
        cursor.close()