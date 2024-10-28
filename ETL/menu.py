import datos as dt
import view as vw
import table as tb


def menu_scripts():
    menu = """------ MENU ------
    1. Migrar a STAGING
    2. Crear Vistas
    3. Migrar vistas al WareHouse
    4. Salir del menu

    Ingrese una opcion: ..."""



    while True:
        opcion = input(menu)
        if opcion in ("1","2","3"):
            print("\n")
            if opcion == "1":
                dt.migrate_oracle_to_sql()
                dt.migrate_sql_to_sql()
            elif opcion == "2":
                vw.clear_views()
            else:
                tb.migrate_view_to_table()
        elif opcion == ("4"):
            break
        else:
            print("Ingrese una opcion valida")

menu_scripts()