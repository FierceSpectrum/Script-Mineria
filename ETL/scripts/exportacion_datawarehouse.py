"""
Módulo de Exportación de Datos al Data Warehouse

Este módulo permite exportar vistas de una base de datos SQL Server al Data Warehouse,
creando o actualizando tablas en el Data Warehouse con datos de las vistas especificadas
en un archivo de configuración JSON. El módulo incluye la creación de tablas con sus relaciones
foráneas, mapeo de identificadores externos y manejo de restricciones.

Funcionalidades principales:
- `migrate_view_to_table(connection)`: Exporta una vista a una tabla en el Data Warehouse,
  creando la tabla en el destino si no existe, ajustando su estructura según el origen,
  y transfiriendo los datos.

Uso:
Este módulo está diseñado para ejecutarse de forma independiente como script, o puede ser
importado en otros scripts para realizar exportaciones al Data Warehouse mediante la función
`migrate_view_to_table`.

Requerimientos:
- Configuración JSON (`config_tablas.json`) con la lista de vistas a exportar y las
  referencias de columnas.
- Conexiones a las bases de datos (configuradas en `conexion_singleton`).
- Módulos de utilidades para manejo de datos, consultas, análisis y relaciones.
"""

# Importaciones de librerías y módulos
import sys
# Añade el directorio ETL al path para importar módulos personalizados
sys.path.append("c:/ETLS/Script-Mineria/ETL")

from utilidades import mnd, rlc, tdc, anl, csl, frt
from conexiones import oradbconn, target_conn, target_conn2, target_conn3


def migrate_view_to_table(connection=target_conn):
    """
    Exporta una vista a una tabla en el Data Warehouse (SQL Server).

    Parámetros:
    - connection: Conexión a la base de datos de origen (por defecto `target_conn`).

    Pasos de la función:
    1. Lee las vistas a migrar desde el archivo de configuración JSON.
    2. Para cada vista especificada en el JSON:
       a. Verifica que la vista existe en la base de datos de origen.
       b. Si la vista no existe, imprime un mensaje y pasa a la siguiente.
       c. Si la tabla de destino no existe en el Data Warehouse, crea la estructura de la tabla.
       d. Ajusta los tipos de datos de las columnas en caso de que existan referencias especificadas.
       e. Comprueba que la estructura de la tabla de origen coincide con la del destino, y si no es así, omite la migración.
       f. Si la estructura coincide y existen datos:
          - Configura las relaciones foráneas si existen referencias.
          - Desactiva restricciones en la tabla de destino, limpia los datos antiguos y carga los nuevos.
          - Activa de nuevo las restricciones.
    """
    connection_wh = target_conn3  # Conexión al Data Warehouse de destino

    # Lee el archivo JSON con la configuración de las vistas a migrar
    views = frt.read_json(
        r'c:/ETLS/Script-Mineria/ETL/config/config_tablas.json')

    # Itera sobre cada vista especificada en el JSON
    for vista in views['tables']:
        name = vista["name"]
        # Verifica existencia de la vista en origen
        exists = csl.table_exists(name, 'dbo')
        if not exists:
            print(f"No existe la vista {name}")
            continue

        # Extrae datos de la vista en origen
        datos = csl.get_table_data('dbo', name, connection)
        # Verifica existencia en destino
        exists = csl.table_exists(name, 'dbo', connection=connection_wh)

        if not exists:
            # Obtiene la estructura de la vista de origen y la crea como tabla en el destino
            table_structure = csl.get_table_structure(
                name, "dbo", "sqlserver", connection)

            # Ajuste de tipos de columnas según referencias especificadas en el JSON
            if "references" in vista:
                columns_table = [coln for coln, _ in table_structure]
                for reference in vista["references"]:
                    column = reference["column"]
                    if column in columns_table:
                        position = columns_table.index(column)
                        table_structure[position] = (column, "int")

            # Crea la tabla en el Data Warehouse
            mnd.create_table(name, 'dbo', table_structure,
                             connection=connection_wh, autoincrementalid=True)
            exists = csl.table_exists(name, 'dbo', connection=connection_wh)

        elif "references" in vista:
            # Verifica que la estructura de columnas coincida en origen y destino
            references = [ref["column"] for ref in vista["references"]]
            table_structure = csl.get_table_structure(
                name, "dbo", "sqlserver", connection)
            table_structure2 = csl.get_table_structure(
                name, "dbo", "sqlserver", connection_wh)
            table_structure2.pop(0)
            lista_dif = [colmn[0] for colmn in table_structure2 if not (
                colmn in table_structure)]
            lista_val = [colmn for colmn in lista_dif if not (
                colmn in references)]
            if len(lista_val) != 0:
                print(
                    f"No se pueden insertar datos ya que la estructura de las tablas {name} no coinciden")
                print(table_structure)
                print(table_structure2)
                continue

        else:
            # Compara las estructuras si no hay referencias especificadas
            table_structure = csl.get_table_structure(
                name, "dbo", "sqlserver", connection)
            table_structure2 = csl.get_table_structure(
                name, "dbo", "sqlserver", connection_wh)
            table_structure2.pop(0)
            if table_structure != table_structure2:
                print(
                    f"No se pueden insertar datos ya que la estructura de las tablas {name} no coinciden")
                print(table_structure)
                print(table_structure2)
                continue

        if datos and exists:
            if "references" in vista:
                # Configura relaciones foráneas si hay referencias
                for reference in vista["references"]:
                    realationship = rlc.has_foreign_key_relationship(
                        name, reference["table_ref"])
                    if not realationship:
                        reference_script = rlc.create_reference_sql(
                            name, reference["column"], reference["table_ref"], reference["foreignKey"]
                        )
                        csl.execute_sql_query(
                            reference_script, connection=connection_wh)

                    # Actualiza los datos de las columnas referenciadas
                    structura = [coln for coln, _ in csl.get_table_structure(
                        name, "dbo", "sqlserver", connection)]
                    structura_ref_table = [coln for coln, _ in csl.get_table_structure(
                        reference['table_ref'], "dbo", "sqlserver", connection)]
                    column = reference["column"]

                    if "DB_ORIGIN" in structura_ref_table:
                        consulta = f"SELECT {reference['foreignKey'].split('_')[0]}_ID, {reference['foreignKey']}, DB_ORIGIN FROM {reference['table_ref']}"
                        ids = csl.get_data_query(consulta, connection_wh)
                        data_dict = {}

                        for key, value, origin in ids:
                            if origin not in data_dict:
                                data_dict[origin] = {}
                            data_dict[origin][key] = value

                        if column in structura:
                            posicion = structura.index(column)
                            datos = rlc.chage_data_for_id_and_origin(
                                datos, posicion, data_dict)

                    else:
                        consulta = f"SELECT {reference['foreignKey'].split('_')[0]}_ID, {reference['foreignKey']} FROM {reference['table_ref']}"
                        ids = csl.get_data_query(consulta, connection_wh)
                        if column in structura:
                            posicion = structura.index(column)
                            datos = rlc.chage_data_for_id(
                                datos, posicion, dict(ids))

            # Desactiva restricciones, limpia la tabla destino y carga los datos
            relacion, realacion_tabla = rlc.has_realtionship(name)
            if relacion:
                csl.disable_restrictions(realacion_tabla)
                mnd.delete_data_entity(name, 'DELETE', connection_wh)
            else:
                mnd.delete_data_entity(name, 'TRUNCATE', connection_wh)
            cant_columns = csl.count_columns(name, "sqlserver", connection_wh) - 1
            mnd.add_data_entity(datos, name, cant_columns, connection_wh)
            csl.enable_restrictions(name)
            
        print("\n") # Espacio para separar logs de cada vista


# Punto de entrada principal del módulo
if __name__ == "__main__":
    # Ejecuta la migración de vistas a tablas en el Data Warehouse
    migrate_view_to_table()
