"""
Módulo de Menú de Scripts

Este módulo presenta un menú interactivo que permite ejecutar varios scripts 
de ETL para migración de datos, creación de vistas y administración de 
tablas en el Data Warehouse. El menú es escalable, de forma que permite 
agregar o modificar opciones fácilmente.

Funcionalidades principales:
- `menu_scripts()`: Muestra un menú de opciones y permite al usuario ejecutar
  la función correspondiente a cada opción.

Uso:
Para utilizar el menú, simplemente ejecuta el módulo y sigue las instrucciones.
Para agregar nuevas opciones al menú, se puede actualizar el diccionario 
`opciones_menu` agregando una nueva clave con la descripción y función 
correspondiente.

Estructura de `opciones_menu`:
- Cada entrada en `opciones_menu` es una opción del menú y tiene las claves:
    - `descripcion`: Texto que describe la opción en el menú.
    - `funcion`: Función a ejecutar cuando se selecciona la opción.

Ejemplo de agregar una opción:
Para agregar una nueva opción, simplemente añade una nueva entrada en 
`opciones_menu` de la siguiente manera:
```python
opciones_menu["6"] = {
    "descripcion": "Nueva opción",
    "funcion": nueva_funcion_a_ejecutar
}
```
"""
# Importación de módulos necesarios
from scripts import migrate_view_to_table, migrate_oracle_to_sql, migrate_sql_to_sql, clear_views, create_tables_wh

# Diccionario de opciones del menú
opciones_menu = {
    "1": {
        "descripcion": "Migrar a STAGING",
        "funcion": lambda: (migrate_oracle_to_sql(), migrate_sql_to_sql())
    },
    "2": {
        "descripcion": "Crear Vistas",
        "funcion": clear_views
    },
    "3": {
        "descripcion": "Migrar vistas al WareHouse",
        "funcion": migrate_view_to_table
    },
    "4": {
        "descripcion": "Montar tablas del WareHouse",
        "funcion": create_tables_wh
    },
    "5": {
        "descripcion": "Salir del menú",
        "funcion": None  # Salir del menú
    }
}

# Función de menú


def menu_scripts():
    """ Función que muestra un menú interactivo basado en el diccionario opciones_menu.
    ```python
    Pasos:
    1. Muestra un menú con las opciones disponibles en `opciones_menu`.
    2. Espera a que el usuario seleccione una opción.
    3. Ejecuta la función asociada a la opción seleccionada, o sale del menú si se elige la opción de salida.
    4. Si la opción seleccionada no es válida, muestra un mensaje de error.

    El menú es escalable; para añadir opciones adicionales, solo es necesario
    modificar `opciones_menu`.
    ```
    """
    # Construcción dinámica del menú de opciones
    menu = "------ MENÚ ------\n"
    for clave, valor in opciones_menu.items():
        menu += f"{clave}. {valor['descripcion']}\n"
    menu += "\nIngrese una opción: "

    # Bucle del menú
    while True:
        opcion = input(menu)
        if opcion in opciones_menu:
            print("\n")
            # Ejecuta la función asociada a la opción si existe
            if opciones_menu[opcion]["funcion"]:
                opciones_menu[opcion]["funcion"]()
            else:
                # Opción para salir
                break
        else:
            print("Ingrese una opción válida")


menu_scripts()
