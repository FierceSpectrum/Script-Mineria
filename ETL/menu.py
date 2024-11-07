import scripts.extraccion_datos as dt
import scripts.transformacion_vistas as vw
import scripts.exportacion_datawarehouse as tb
import scripts.warehouse as wh

def menu_scripts():
    menu = """------ MENU ------
    1. Migrar a STAGING
    2. Crear Vistas
    3. Migrar vistas al WareHouse
    4. Montar tablas del WareHouse
    5. Salir del menu

    Ingrese una opcion: ..."""



    while True:
        opcion = input(menu)
        if opcion in ("1","2","3","4"):
            print("\n")
            if opcion == "1":
                dt.migrate_oracle_to_sql()
                dt.migrate_sql_to_sql()
            elif opcion == "2":
                vw.clear_views()
            elif opcion == "3":
                tb.migrate_view_to_table()
            elif opcion == "4":
                wh.create_tables_wh()
            else:
                pass
        elif opcion == ("5"):
            break
        else:
            print("Ingrese una opcion valida")

menu_scripts()