import utilidades as utl


def clear_views(connection=utl.target_conn):
    """Limpia y transforma los datos de las tablas de acuerdo al archivo JSON."""
    datos = utl.read_json(r'c:/ETLS/Script-Mineria/ETL/views.json')
    tablas = datos["views"]

    for tabla in tablas:
        name = tabla['name'].upper()
        table_name = f"CLEAR_{name}"
        datos = utl.execute_sql_query(tabla["query"])

        exists = utl.table_exists(name, "dbo")
        if not exists:
            view_query = f"CREATE OR ALTER VIEW {name} AS SELECT * FROM ({tabla['query']}) AS V;"
            utl.execute_sql_view(view_query)
            exists = utl.table_exists(name, "dbo")

        table_structure = utl.get_table_structure(
            name, "dbo", "sqlserver", connection)

        exists = utl.table_exists(table_name, "dbo")
        if not exists:
            utl.create_table(table_name, "dbo", table_structure)
            exists = utl.table_exists(table_name, "dbo")
        else:
            table_structure2 = utl.get_table_structure(
            table_name, "dbo", "sqlserver", connection)
            if table_structure != table_structure2:
                print(f"No se pudes insertar datos ya que la estructura de las tablas {name} y {table_name} no coisiden")
                continue
            

        datos = utl.clear_data_sql(tabla, table_structure, connection)

        if datos and exists:
            utl.delete_data_entity(table_name, 'TRUNCATE')
            cant_columns = utl.count_columns(
                table_name, "sqlserver", connection)
            utl.add_data_entity(datos, table_name, cant_columns)
            view_query = f"CREATE OR ALTER VIEW {name} AS SELECT * FROM {table_name};"
            utl.execute_sql_view(view_query)

        print("\n")


clear_views()
