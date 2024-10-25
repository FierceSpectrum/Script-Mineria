import utilidades as utl


def clear_views(connection=utl.target_conn):
    """Limpia y transforma los datos de las tablas de acuerdo al archivo JSON."""
    datos = utl.read_json(r'c:/ETLS/Script-Mineria/ETL/tables.json')
    tablas = datos["tables"]

    for tabla in tablas:
        name = tabla['name']
        table_name = f"clear_{name}"
        datos = utl.execute_sql_query(tabla["query"])

        exists = utl.table_exists(name, "dbo")
        if not exists:
            view_query = f"CREATE OR ALTER VIEW {name} AS SELECT * FROM ({tabla['query']}) AS V;"
            utl.execute_sql_view(view_query)
            exists = utl.table_exists(name, "dbo")

        table_structure = utl.get_table_structure(
            name, "dbo", "sqlserver", connection)

        exists = utl.table_exists(table_name, "dbo")
        if (not exists):
            utl.create_table(table_name, "dbo", table_structure)
            exists = utl.table_exists(table_name, "dbo")

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
