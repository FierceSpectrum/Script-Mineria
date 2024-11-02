import utilidades as utl


def migrate_view_to_table(connection=utl.target_conn):
    connection_wh = utl.target_conn3
    # Leer archivo JSON
    views = utl.read_json(r'c:/ETLS/Script-Mineria/ETL/tables.json')

    for vista in views['tables']:
        name = vista["name"]
        exists = utl.table_exists(name, 'dbo')
        if not exists:
            print(f"No existe la vista {name}")
            continue
        datos = utl.get_table_data('dbo', name, connection)

        exists = utl.table_exists(name, 'dbo', connection=connection_wh)
        if not exists:
            table_structure = utl.get_table_structure(
                name, "dbo", "sqlserver", connection)
            utl.create_table(name, 'dbo', table_structure,
                             connection=connection_wh, autoincrementalid=True)
            exists = utl.table_exists(name, 'dbo', connection=connection_wh)

            if "references" in vista:
                references_list = vista["references"]
                for reference in references_list:
                    reference_script = utl.create_reference_sql(
                        name, reference["column"], reference["table_ref"], reference["foreignKey"])
                    utl.execute_sql_query(reference_script)

        else:
            table_structure = utl.get_table_structure(
                name, "dbo", "sqlserver", connection)
            table_structure2 = utl.get_table_structure(
                name, "dbo", "sqlserver", connection_wh)
            table_structure2.pop(0)
            # if table_structure2[0][1] == "" else table_structure2
            if table_structure != table_structure2:
                print(
                    f"No se pudes insertar datos ya que la estructura de las tablas {name} no coisiden")
                print(table_structure)
                print(table_structure2)
                continue

        if datos and exists:
            utl.delete_data_entity(name, 'TRUNCATE', connection_wh)
            cant_columns = utl.count_columns(
                name, "sqlserver", connection_wh) - 1
            utl.add_data_entity(datos, name, cant_columns, connection_wh)

        print("\n")


if __name__ == "__main__":
    migrate_view_to_table()
