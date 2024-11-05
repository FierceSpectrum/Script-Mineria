from utilidades import formato as frt
from conexiones.conexion_singleton import oradbconn, target_conn, target_conn2, target_conn3

from translate import Translator
translator = Translator(from_lang="en", to_lang="es")


def translate_phrase(frase):
    traduccion = translator.translate(frase)
    return traduccion


def data_translate(data, columns, table_structure):
    """Traduce los datos en las columnas especificadas."""

    positions = [table_structure.index(col)
                 for col in columns if col in table_structure]

    datos_a_traducir = {file[pos]
                        for file in data for pos in positions if file[pos]}
    datos_sin_duplicados = frt.eliminar_duplicados(list(datos_a_traducir))

    # Divide en sublistas de tamaño 10
    sublistas = frt.dividir_lista_en_sublistas(datos_sin_duplicados, 10)

    # Traducir los datos
    sublistas_traducidas = [translate_phrase(
        ", ".join(sublista)).split(", ") for sublista in sublistas]

    # Unir las sublistas traducidas
    traducciones = sum(sublistas_traducidas, [])

    # Reemplazar los datos en las posiciones correspondientes
    for file in data:
        for pos in positions:
            valor = file[pos]
            if valor:
                file[pos] = traducciones[datos_sin_duplicados.index(valor)]
    return data
