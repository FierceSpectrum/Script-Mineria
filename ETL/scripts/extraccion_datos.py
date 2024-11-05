import sys
sys.path.append("c:/ETLS/Script-Mineria/ETL")

from utilidades import analisis as anl, consultas as csl, formato as frt
from utilidades import manejo_datos as mnd, relaciones as rlc, traducciones as tdc
from conexiones.conexion_singleton import oradbconn, target_conn, target_conn2, target_conn3



def migrate_sql_to_sql(connection=target_conn2):
    tablas = csl.get_user_tables("sqlserver", target_conn2)
    anl.check_tables(tablas)
    for tabla in tablas:

        exists = csl.table_exists(tabla, "dbo")
        datos = csl.get_table_data('dbo', tabla, target_conn2)

        if not exists:
            table_structure = csl.get_table_structure(
                tabla, "dbo", "sqlserver", target_conn2)
            mnd.create_table(tabla, "dbo", table_structure)
            exists = csl.table_exists(tabla, "dbo")
        if datos and exists:
            mnd.delete_data_entity(tabla, 'TRUNCATE')
            cant_columns = csl.count_columns(
                tabla, "sqlserver", target_conn2)
            mnd.add_data_entity(datos, tabla, cant_columns)
        else:
            print(f"Error obteniendo datos de la tabla {tabla}")

        print("\n")


def migrate_oracle_to_sql():
    tablas = csl.get_user_tables()
    anl.check_tables(tablas)
    for tabla in tablas:

        exists = csl.table_exists(tabla, "dbo")
        datos = csl.get_table_data('jardineria', tabla)

        if not exists:
            table_structure = csl.get_table_structure(tabla, "JARDINERIA")
            mnd.create_table(tabla, "dbo", table_structure)
            exists = csl.table_exists(tabla, "dbo")
        if datos and exists:
            mnd.delete_data_entity(tabla, 'TRUNCATE')
            cant_columns = csl.count_columns(tabla)
            mnd.add_data_entity(datos, tabla, cant_columns)
        else:
            print(f"Error obteniendo datos de la tabla {tabla}")

        print("\n")


if __name__ == "__main__":
    migrate_oracle_to_sql()
    migrate_sql_to_sql()
