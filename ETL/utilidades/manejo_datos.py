from utilidades import formato as frt, consultas as cst
from conexiones.conexion_singleton import oradbconn, target_conn, target_conn2, target_conn3

def add_data_entity(data, tablename, numfields, connection=target_conn):
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
    cursor = oradbconn.cursor()
    try:
        cursor.callproc(pprocedure)
        oradbconn.commit()  # Confirma los cambios si el procedimiento realiza modificaciones
    except Exception as e:
        print(f"Error al ejecutar el procedimiento {pprocedure}: {str(e)}")
    finally:
        cursor.close()


def create_table(table_name, owner, table_structure, db_type="sqlserver", connection=target_conn, autoincrementalid=False):
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
