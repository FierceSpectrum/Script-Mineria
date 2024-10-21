import utilidades as utl

def check_tables(tables):
    if isinstance(tables, list) and len(tables) > 0:
        print(f"Tablas encontradas: {tables}")
        pass
    else:
        print("No se encontraron tablas o el valor no es válido.")
        exit()

def clear_data_sql(connection=utl.target_conn):
    tablas = utl.get_user_tables("sqlserver", connection)
    check_tables(tablas)
    for tabla in tablas:
        table_name = f"clear_{tabla}"

        exists = utl.table_exists(table_name, "dbo")
        datos = utl.get_table_data('dbo', tabla,connection)

        if not exists:
            table_structure = utl.get_table_structure(
                tabla, "dbo", "sqlserver", connection)
            utl.create_table(table_name, "dbo", table_structure)
            exists = utl.table_exists(table_name, "dbo")
        if datos and exists:
            utl.delete_data_entity(table_name, 'TRUNCATE')
            cant_columns = utl.count_columns(
                table_name, "sqlserver", connection)
            
            table_structure = utl.get_table_structure(
                tabla, "dbo", "sqlserver", connection)
            # print(table_structure)

            positions = [] 
            columns = []
            for position, clumns in enumerate(table_structure):
                clumn, type = clumns
                if "nvarchar" in type.lower() or "ncahr" in type.lower():
                    positions.append(position)
                    columns.append(clumn)
            print(positions,columns)

            for position in positions:
                for row in datos:
                    frase = row[position]
                    frase_traducida = utl.translate_phrase(frase)
                    print(f"{frase} - - {frase_traducida}")

            # utl.add_data_entity(datos, table_name, cant_columns)
            break
        else:
            print(f"Error obteniendo datos de la tabla {tabla}")

        print("\n")

clear_data_sql()

