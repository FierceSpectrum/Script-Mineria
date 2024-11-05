import pandas as pd
import json
from utilidades import consultas as cst, traducciones as tdc
from conexiones.conexion_singleton import oradbconn, target_conn, target_conn2, target_conn3


def convert_data_as_dataframe(data, table_name):
    _, column_names = cst.table_attributes(table_name)
    if data and column_names:
        # Convertir los datos en un DtaFrame de pandas
        df = pd.DataFrame(data, columns=column_names)
        return df
    else:
        print(
            f"No hay datos para convertir")
        return None


def read_json(rute="C:/ETLS/Script-Mineria/ETL/config/config_vistas.json"):
    with open(rute, 'r') as file:
        data = json.load(file)
    return data


def clear_data_sql(tabla, table_structure, connection=target_conn):
    """Limpia y transforma los datos de la tabla."""

    name = tabla['name']
    datos = cst.get_data_query(tabla["query"])

    if not datos:
        print(f"Error obteniendo datos de la tabla {name}")
        return

    list_column_table = [col for col, _ in table_structure]

    # Transformar datos según las columnas configuradas
    if "country" in tabla:
        datos = data_transform(datos, [col.upper() for col in tabla["country"] if col], list_column_table, lambda x: read_json(
            r'C:/ETLS/Script-Mineria/ETL/config/countrys.json').get(x, x))

    if "translate" in tabla:
        datos = tdc.data_translate(
            datos, [col.upper() for col in tabla["translate"] if col], list_column_table)

    if "upper" in tabla:
        datos = data_transform(
            datos, [col.upper() for col in tabla["upper"] if col], list_column_table, str.upper)

    return datos


def convert_data_type(data_type, db_type):
    if db_type.lower() == "oracle":
        if "INT" in data_type:
            return "NUMBER(10)"
        elif "DECIMAL" in data_type:
            return "NUMBER(15,2)"
        elif "NVARCHAR" in data_type:
            return data_type.replace("NVARCHAR", "VARCHAR2")
        elif "NCHAR" in data_type:
            return data_type.replace("NCHAR", "CHAR")
    elif db_type.lower() == "sqlserver":
        if "NUMBER" in data_type:
            if "(" in data_type and not ("(10)" in data_type):  # Caso de NUMBER con precisión
                return data_type.replace("NUMBER", "DECIMAL")
            return "INT"  # Caso de NUMBER sin precisión
        elif "VARCHAR2" in data_type:
            return data_type.replace("VARCHAR2", "NVARCHAR")
        elif "CHAR" in data_type:
            return data_type.replace("CHAR", "NCHAR")
        elif "nvarchar(-1)" in data_type:
            return data_type.replace("-1", "max")
        # elif "image" in data_type:
        #     return "varbinary(max)"

    return data_type


def data_transform(data, columns, table_structure, transform_func):
    """Aplica una transformación a las columnas especificadas."""
    positions = [table_structure.index(col)
                 for col in columns if col in table_structure]
    for file in data:
        for pos in positions:
            if file[pos]:
                file[pos] = transform_func(file[pos])
    return data


def dividir_lista_en_sublistas(lista, y):
    """Divide las listas en sublistas."""
    return [lista[i:i + y] for i in range(0, len(lista), y)]


def eliminar_duplicados(lista):
    """Elimina duplicados de una lista manteniendo el orden."""
    seen = set()
    return [item for item in lista if not (item in seen or seen.add(item))]
