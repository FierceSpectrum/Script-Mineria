import utilidades as utl


def migrate_view_to_table(connection=utl.target_conn):
    connection_wh = utl.target_conn3
    # Leer archivo JSON
    views = utl.read_json(r'c:/ETLS/Script-Mineria/ETL/tables.json')

    for vista in views['tables']:
        exists = utl.table_exists(vista, 'dbo')
        if not exists:
            print(f"No existe la vista {vista}")
            continue
        datos = utl.get_table_data('dbo', vista, connection)

        exists = utl.table_exists(vista, 'dbo', connection=connection_wh)
        if not exists:
            table_structure = utl.get_table_structure(
                vista, "dbo", "sqlserver", connection)
            utl.create_table(vista, 'dbo', table_structure,
                             connection=connection_wh, autoincrementalid=True)
            exists = utl.table_exists(vista, 'dbo', connection=connection_wh)

        else:
            table_structure = utl.get_table_structure(
                vista, "dbo", "sqlserver", connection)
            table_structure2 = utl.get_table_structure(
                vista, "dbo", "sqlserver", connection_wh)
            table_structure2.pop(0)
            if table_structure != table_structure2:
                print(
                    f"No se pudes insertar datos ya que la estructura de las tablas {vista} no coisiden")
                print(table_structure)
                print(table_structure2)
                continue

        if datos and exists:
            utl.delete_data_entity(vista, 'TRUNCATE', connection_wh)
            cant_columns = utl.count_columns(
                vista, "sqlserver", connection_wh) -1
            utl.add_data_entity(datos, vista, cant_columns, connection_wh)
        # print(datos)


migrate_view_to_table()
