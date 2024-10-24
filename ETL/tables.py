import utilidades as utl
# import itertools
import pycountry


def check_tables(tables):
    if isinstance(tables, list) and len(tables) > 0:
        print(f"Tablas encontradas: {tables}")
        pass
    else:
        print("No se encontraron tablas o el valor no es válido.")
        exit()

def dividir_lista_en_sublistas(lista, y):
    return [lista[i:i + y] for i in range(0, len(lista), y)]

def eliminar_duplicados(lista):
    lista_sin_duplicados = []
    seen = set()  # Conjunto para rastrear elementos ya vistos
    for item in lista:
        if item not in seen:
            lista_sin_duplicados.append(item)
            seen.add(item)
    return lista_sin_duplicados

def unificar_listas(lista):
    return [item for sublista in lista for item in sublista]

def clear_data_sql(connection=utl.target_conn):

    # Leer archivo JSON
    datos = utl.read_json(r'c:/ETLS/Script-Mineria/ETL/tables.json')

    # tablas = utl.get_user_tables("sqlserver", connection)
    tablas = datos["tables"]
    # check_tables(tablas)
    for tabla in tablas:
        name = tabla['name']
        table_name = f"clear_{tabla['name']}"
        exists = utl.table_exists(table_name, "dbo")
        datos = utl.execute_sql_query(tabla["query"])
        if not exists:
            table_structure = utl.get_table_structure(
                name, "dbo", "sqlserver", connection)
            utl.create_table(table_name, "dbo", table_structure)
            exists = utl.table_exists(table_name, "dbo")
        if datos and exists:
            utl.delete_data_entity(table_name, 'TRUNCATE')
            cant_columns = utl.count_columns(
                table_name, "sqlserver", connection)
            

            table_structure = utl.get_table_structure(
                name, "dbo", "sqlserver", connection)
            

            # print(table_structure)

            """
            [('employee_id', 'int'), ('full_name', 'nvarchar(101)'), ('title', 'nvarchar(50)'), ('city', 'nvarchar(30)'), ('region', 'nvarchar(50)'), ('country', 'nvarchar(50)'), ('reports_to', 'nvarchar(101)')]
            """

            # print(datos)

            """
            (1, 'Marcos Magaña', 'Director General', 'Talavera de la Reina', 'Castilla-LaMancha', 'España', 'COMITE GERENCIAL')
            """
            columnas_tables = tabla["columnas_traductor"]
            positions = []
            columns = []
            for position, clumns in enumerate(table_structure):
                clumn, type = clumns
                if clumn in columnas_tables:
                    positions.append(position)
                    columns.append(clumn)

            list_column_table = [columns for columns, _ in table_structure]
            columnas = tabla["country"]
            if columnas:
                valid_columns = [column for column in columnas if column in list_column_table ]
                if valid_columns:
                    country_json = utl.read_json(r'c:/ETLS/Script-Mineria/ETL/countrys.json')
                    for position, file in enumerate(datos):
                        for colmn in valid_columns:
                            pos = list_column_table.index(colmn)
                            dato = file[pos]
                            if dato in country_json:
                                file[pos] = country_json[dato]
                                datos[position] = file

            columnas = tabla["upper"]
            if columnas:
                valid_columns = [column for column in columnas if column in list_column_table ]
                if valid_columns:
                    for position, file in enumerate(datos):
                        for colmn in valid_columns:
                            pos = list_column_table.index(colmn)
                            dato = file[pos]
                            if dato:
                                file[pos] = dato.upper()
                                datos[position] = file

                print(datos)

            
            lista_traduccion = [ data for dato in datos for position, data in enumerate(dato) if position in positions]

            # print(lista_traduccion)

            lista_sin_duplicados = eliminar_duplicados(lista_traduccion)
            sublistas = dividir_lista_en_sublistas(lista_sin_duplicados, 10)

            # print(sublistas)

            # Traducir los datos en sublistas
            sublistas_traducidas = []
            for lista in sublistas:
                traducir = ", ".join(lista)
                frase_traducida = utl.translate_phrase(traducir)
                sublistas_traducidas.append(frase_traducida.split(", "))


            lista_unida = sum(sublistas_traducidas, [])

            # print(datos)

            # Reemplazar los datos en las posiciones correspondientes
            for i, fila in enumerate(datos):
                fila_modificada = list(fila)
                for pos in positions:
                    dato = fila_modificada[pos]
                    if dato:
                        position = lista_sin_duplicados.index(dato)
                        fila_modificada[pos] = lista_unida[position]
                datos[i] = fila_modificada
            
            print(datos)

            # Format data

           
            # countries_dict = {}

            # for country in pycountry.countries:
            #     # Nombres del país y variantes
            #     country_names = [country.name]  # Nombre oficial
            #     if hasattr(country, 'official_name'):
            #         country_names.append(country.official_name)  # Nombre oficial completo

            #     # Añadir acrónimos
            #     country_names.append(country.alpha_2)  # ISO Alpha-2
            #     country_names.append(country.alpha_3)  # ISO Alpha-3

            #     # Añadir el país y sus variantes al diccionario
            #     countries_dict[country.alpha_2] = country_names

            # print(countries_dict)


            
            # print(f"{traducir} - - {frase_traducida}")
            # print(sublistas_traducidas)


            # positions = tabla["columnas_traductor_position"]
            # columns = [column[0] for position, column in enumerate(table_structure) if position in positions]

            # for position in positions:
            #     val = [f"{x if y in position else None }" for y, x in enumerate(datos[position])]
            #     print(", ".join(val))
            #     # for row in datos:
            #     #     frase = row[position]
            #     frase_traducida = utl.translate_phrase(val)
            #     print(f"{val} - - {frase_traducida}")

            utl.add_data_entity(datos, table_name, cant_columns)
            break
        else:
            print(f"Error obteniendo datos de la tabla {tabla}")

        print("\n")


clear_data_sql()

# custom_aliases = {
#     # "EEUU": "United States",
#     # "US": "United States",
#     "EU": "United States",
#     "EEUU": "United States",
#     "USA": "United States",
#     # Añadir más alias personalizados
# }

# def get_country_name(query):
#     # Primero buscar en los alias personalizados
#     if query in custom_aliases:
#         query = custom_aliases[query]

#     # Luego buscar en pycountry
#     try:
#         country = pycountry.countries.lookup(query)
#         return country.name
#     except LookupError:
#         return None

# # Ejemplo de uso
# print(get_country_name("US"))       # 'United States'
# print(get_country_name("United States"))




# import country_converter as coco

# # Convertir nombres alternativos o acrónimos a nombres oficiales
# def get_country_name(query):
#     return coco.convert(names=query, to='name_short')

# # Ejemplo de uso
# print(get_country_name("US"))       # 'United States'
# print(get_country_name("EE.UU."))   # 'United States'
# print(get_country_name("DE"))       # 'Germany'
# print(get_country_name("México"))   # 'Mexico'
