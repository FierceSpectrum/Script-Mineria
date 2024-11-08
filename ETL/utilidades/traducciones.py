"""
Módulo de Traducciones Automáticas

Este módulo proporciona funciones para traducir frases y datos en tablas automáticamente. Utiliza
una herramienta de traducción para transformar datos en inglés a español y se integra con otros
módulos para manejar los datos a traducir.

Funcionalidades principales:
- `translate_phrase(frase)`: Traduce una frase del inglés al español.
- `data_translate(data, columns, table_structure)`: Traduce los datos en las columnas especificadas de una tabla.

Requerimientos:
- `translate`: Biblioteca de traducción para realizar traducciones automáticas.
- Módulo `formato` para eliminar duplicados y dividir listas.
"""

from . import formato as frt
from translate import Translator

translator = Translator(from_lang="en", to_lang="es")


def translate_phrase(frase):
    """
    Traduce una frase de un idioma a otro utilizando un traductor automatizado.

    Parámetros:
    - frase (str): La frase que se desea traducir.

    Retorno:
    str: La traducción de la frase en el idioma de destino.
    """
    traduccion = translator.translate(frase)
    return traduccion


def data_translate(data, columns, table_structure):
    """
    Traduce los datos de las columnas especificadas en una tabla, usando un traductor automatizado para las entradas de texto.

    Parámetros:
    - data (list of list): Lista de listas donde cada sublista representa una fila de datos.
    - columns (list of str): Lista de nombres de columnas cuyas celdas deben ser traducidas.

    Retorno:
    list of list: Los datos con las celdas de las columnas especificadas traducidas.
    """

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
