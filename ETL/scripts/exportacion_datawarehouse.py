# exportacion_datawarehouse.py
import sys
sys.path.append("c:/ETLS/Script-Mineria/ETL")


from utilidades import analisis as anl, consultas as csl, formato as frt
from utilidades import manejo_datos as mnd, relaciones as rlc, traducciones as tdc
from conexiones.conexion_singleton import oradbconn, target_conn, target_conn2, target_conn3



def migrate_view_to_table(connection=target_conn):
    connection_wh = target_conn3
    # Leer archivo JSON
    views = frt.read_json(r'c:/ETLS/Script-Mineria/ETL/config/config_tablas.json')

    for vista in views['tables']:
        name = vista["name"]
        exists = csl.table_exists(name, 'dbo')
        if not exists:
            print(f"No existe la vista {name}")
            continue
        datos = csl.get_table_data('dbo', name, connection)

        exists = csl.table_exists(name, 'dbo', connection=connection_wh)

        if not exists:
            table_structure = csl.get_table_structure(
                name, "dbo", "sqlserver", connection)

            if "references" in vista:
                columns_table = [coln for coln, _ in table_structure]
                for reference in vista["references"]:
                    column = reference["column"]
                    if column in columns_table:
                        position = columns_table.index(column)
                        table_structure[position] = (column, "int")

            mnd.create_table(name, 'dbo', table_structure,
                             connection=connection_wh, autoincrementalid=True)
            exists = csl.table_exists(name, 'dbo', connection=connection_wh)

        elif "references" in vista:
            references = [ref["column"] for ref in vista["references"]]
            table_structure = csl.get_table_structure(
                name, "dbo", "sqlserver", connection)
            table_structure2 = csl.get_table_structure(
                name, "dbo", "sqlserver", connection_wh)
            table_structure2.pop(0)
            lista_dif = [colmn[0] for colmn in
                         table_structure2 if not (colmn in table_structure)]
            lista_val = [colmn for colmn in lista_dif if not (
                colmn in references)]
            if len(lista_val) != 0:
                print(
                    f"No se pudes insertar datos ya que la estructura de las tablas {name} no coisiden")
                print(table_structure)
                print(table_structure2)
                continue

        else:
            table_structure = csl.get_table_structure(
                name, "dbo", "sqlserver", connection)
            table_structure2 = csl.get_table_structure(
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
            if "references" in vista:

                for reference in vista["references"]:

                    realationship = rlc.has_foreign_key_relationship(
                        name, reference["table_ref"])

                    if not realationship:
                        reference_script = rlc.create_reference_sql(
                            name, reference["column"], reference["table_ref"], reference["foreignKey"])
                        csl.execute_sql_query(
                            reference_script, connection=connection_wh)
                    if reference["table_ref"] != "VDIM_DATES":
                        consulta = f"SELECT {reference['foreignKey'].split('_')[0]}_ID, {reference['foreignKey']}, DB_ORIGIN FROM {reference['table_ref']}"
                        ids = csl.get_data_query(consulta, connection_wh)
                        data_dict = {}

                        for key, value, origin in ids:
                            if origin not in data_dict:
                                data_dict[origin] = {}
                            data_dict[origin][key] = value

                        structura = [coln for coln, _ in csl.get_table_structure(
                            name, "dbo", "sqlserver", connection)]
                        column = reference["column"]
                        if column in structura:
                            posicion = structura.index(column)
                            datos = rlc.chage_data_for_id(
                                datos, posicion, data_dict)
                    elif reference["table_ref"] == "VDIM_DATES":
                        pass
                
            relacion, realacion_tabla = rlc.has_realtionship(name)
            if relacion:
                csl.disable_restrictions(realacion_tabla)
                mnd.delete_data_entity(name, 'DELETE', connection_wh)
            else:
                mnd.delete_data_entity(name, 'TRUNCATE', connection_wh)
            cant_columns = csl.count_columns(
                name, "sqlserver", connection_wh) - 1
            mnd.add_data_entity(datos, name, cant_columns, connection_wh)
            csl.enable_restrictions(name)
        print("\n")


if __name__ == "__main__":
    migrate_view_to_table()
