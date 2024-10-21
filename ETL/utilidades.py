import pandas as pd
import conexion as conn

from translate import Translator
translator = Translator(from_lang="es", to_lang="en")

oradbconn = conn.ora_conndb()
target_conn = conn.mssql_conndb(method=1)
target_conn2 = conn.mssql_conndb(DB_DATABASE='DB_DATABASE2')


# Mio

def convert_data_as_dataframe(data, table_name):
    _, column_names = table_attributes(table_name)
    if data and column_names:
        # Convertir los datos en un DtaFrame de pandas
        df = pd.DataFrame(data, columns=column_names)
        return df
    else:
        print(
            f"No hay datos para convertir")
        return None


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
            if data_type in ["VARCHAR2", "CHAR", "nvarchar", "nchar"]:
                table_structure.append(
                    (column_name, f"{data_type}({data_length})"))
            elif data_type in ["NUMBER"]:
                table_structure.append(
                    (column_name, f"{data_type}({data_presicion})"))
            else:
                table_structure.append((column_name, data_type))

        return table_structure
    except Exception as e:
        print(f"Error al obtener la estructura de la tabla: {str(e)}")
        return None

    finally:
        if cursor:
            cursor.close()


def convert_data_type(data_type, db_type):
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


def create_table(table_name, owner, table_structure, db_type="sqlserver", connection=target_conn):
    cursor = connection.cursor()
    try:
        # Contruir la consulta CREATE TABLE
        create_query = f'CREATE TABLE {owner.upper()}."{table_name.upper()}" ('

        column_definitions = []
        for column_name, data_type in table_structure:
            converted_type = convert_data_type(data_type, db_type)
            column_definitions.append(f"{column_name} {converted_type}")

        create_query += ", ".join(column_definitions) + ")"
        cursor.execute(create_query)
        connection.commit()

        print(f"Table {table_name} creada exitosamente en {owner}.")

    except Exception as e:
        print(f"Error al crear la tabla: {str(e)}")

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


def execute_sql_query(sql_query, connection=target_conn):
    cursor = connection.cursor()
    try:
        cursor.execute(sql_query)
        data = cursor.fetchall()
        return data
    except Exception as e:
        print(
            f"Error al obtener datos de la tabla {sql_query.split('.')[1]}: {str(e)}")
    finally:
        if cursor:
            cursor.close()


def execute_sql_view(sql_query, connection=target_conn):
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

def translate_phrase(frase):
    traduccion = translator.translate(frase)
    return traduccion

# Profe


def check_lengths(dataframe, max_length, limit=10):
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    for column in dataframe.columns:
        if dataframe[column].apply(lambda x: len(str(x)) > max_length).any():
            print(f"Columna: {column} contiene valores demasiado largos:")
            print(dataframe[dataframe[column].apply(
                lambda x: len(str(x)) > max_length)].head(limit))
            print("\n")


def max_column_sizes(df):
    column_sizes = df.applymap(lambda x: len(str(x))).max()
    sorted_sizes = column_sizes.sort_values(ascending=False)
    print(sorted_sizes)


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


def execute_oracle_procedure(pprocedure):
    cursor = oradbconn.cursor()
    try:
        cursor.callproc(pprocedure)
        oradbconn.commit()  # Confirma los cambios si el procedimiento realiza modificaciones
    except Exception as e:
        print(f"Error al ejecutar el procedimiento {pprocedure}: {str(e)}")
    finally:
        cursor.close()


def add_data_entity(data, tablename, numfields, connection=target_conn):
    sql = create_sql_insert("dbo", tablename, numfields)

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
