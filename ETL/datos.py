import utilidades as utl


def check_tables(tables):
    if isinstance(tables, list) and len(tables) > 0:
        print(f"Tablas encontradas: {tables}")
        pass
    else:
        print("No se encontraron tablas o el valor no es válido.")
        exit()


def migrate_sql_to_sql(connection=utl.target_conn2):
    tablas = utl.get_user_tables("sqlserver", utl.target_conn2)
    check_tables(tablas)
    for tabla in tablas:

        exists = utl.table_exists(tabla, "dbo")
        datos = utl.get_table_data('dbo', tabla, utl.target_conn2)

        if not exists:
            table_structure = utl.get_table_structure(tabla, "dbo", "sqlserver", utl.target_conn2)
            utl.create_table(tabla, "dbo", table_structure)
            exists = utl.table_exists(tabla, "dbo")
        if datos and exists:
            utl.delete_data_entity(tabla, 'TRUNCATE')
            cant_columns = utl.count_columns(tabla, "sqlserver", utl.target_conn2)
            utl.add_data_entity(datos, tabla, cant_columns)
        else:
            print(f"Error obteniendo datos de la tabla {tabla}")

        print("\n")


def migrate_oracle_to_sql():
    tablas = utl.get_user_tables()
    check_tables(tablas)
    for tabla in tablas:

        exists = utl.table_exists(tabla, "dbo")
        datos = utl.get_table_data('jardineria', tabla)

        if not exists:
            table_structure = utl.get_table_structure(tabla, "JARDINERIA")
            utl.create_table(tabla, "dbo", table_structure)
            exists = utl.table_exists(tabla, "dbo")
        if datos and exists:
            utl.delete_data_entity(tabla, 'TRUNCATE')
            cant_columns = utl.count_columns(tabla)
            utl.add_data_entity(datos, tabla, cant_columns)
        else:
            print(f"Error obteniendo datos de la tabla {tabla}")

        print("\n")

if __name__ == "__main__":
    migrate_oracle_to_sql()
    migrate_sql_to_sql()
