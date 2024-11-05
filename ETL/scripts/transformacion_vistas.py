import sys
sys.path.append("c:/ETLS/Script-Mineria/ETL")

from utilidades import analisis as anl, consultas as csl, formato as frt
from utilidades import manejo_datos as mnd, relaciones as rlc, traducciones as tdc
from conexiones.conexion_singleton import oradbconn, target_conn, target_conn2, target_conn3



def clear_views(connection=target_conn):
    """Limpia y transforma los datos de las tablas de acuerdo al archivo JSON."""
    datos = frt.read_json(r'c:/ETLS/Script-Mineria/ETL/config/config_vistas.json')
    tablas = datos["views"]

    for tabla in tablas:
        name = tabla['name'].upper()
        table_name = f"CLEAR_{name}"
        # datos = utl.execute_sql_query(tabla["query"])

        exists = csl.table_exists(name, "dbo")
        if not exists:
            view_query = f"CREATE OR ALTER VIEW {name} AS SELECT * FROM ({tabla['query']}) AS V;"
            mnd.execute_sql_view(view_query)
            exists = csl.table_exists(name, "dbo")
        
        table_structure = csl.get_table_structure(
            name, "dbo", "sqlserver", connection)

        exists = csl.table_exists(table_name, "dbo")
        if not exists:
            mnd.create_table(table_name, "dbo", table_structure)
            exists = csl.table_exists(table_name, "dbo")
        else:
            table_structure2 = csl.get_table_structure(
            table_name, "dbo", "sqlserver", connection)
            if table_structure != table_structure2:
                print(f"No se pudes insertar datos ya que la estructura de las tablas {name} y {table_name} no coisiden")
                print("\n")
                continue
            

        datos = frt.clear_data_sql(tabla, table_structure, connection)

        if datos and exists:
            mnd.delete_data_entity(table_name, 'TRUNCATE')
            cant_columns = csl.count_columns(
                table_name, "sqlserver", connection)
            mnd.add_data_entity(datos, table_name, cant_columns)
            view_query = f"CREATE OR ALTER VIEW {name} AS SELECT * FROM {table_name};"
            mnd.execute_sql_view(view_query)

        print("\n")

if __name__ == "__main__":
    clear_views()
