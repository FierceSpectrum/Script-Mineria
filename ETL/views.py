import utilidades as utl


def crear_vistas(json_data, connection=utl.target_conn):

    for vista in json_data['vistas']:
        nombre_vista = vista['nombre_vista']
        columnas = vista['columnas']

        consultas_unidas = []

        # Procesar cada fuente de datos (tabla)
        for data in vista['data']:
            tabla = data['tabla']
            columnas_tabla = data['columnas']

            # Generar las selecciones por tabla
            consulta = utl.create_sql_select(
                'dbo', tabla, columnas_tabla.items())
            consultas_unidas.append(consulta)

        # Unir las consultas con UNION
        consulta_final = "\nUNION\n".join(consultas_unidas)

        # Crear la vista
        crear_vista = utl.create_sql_view(
            nombre_vista, columnas, consulta_final)

        print(crear_vista)
        # utl.execute_sql_view(crear_vista)


# Leer archivo JSON
datos = utl.read_json()

# Crear las vistas
crear_vistas(datos)
