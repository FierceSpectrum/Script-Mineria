from utilidades import formato as frt
from conexiones.conexion_singleton import oradbconn, target_conn, target_conn2, target_conn3

from translate import Translator
translator = Translator(from_lang="en", to_lang="es")


def translate_phrase(frase):
    """
    Traduce una frase de un idioma a otro utilizando un traductor automatizado.

    Esta función utiliza un traductor (por ejemplo, Google Translate o cualquier otra API de traducción) para traducir una frase 
    del idioma original al idioma de destino.

    Parámetros:
    frase (str): La frase que se desea traducir.

    Retorno:
    str: La traducción de la frase en el idioma de destino.

    Excepciones:
    Si ocurre un error al intentar traducir la frase (por ejemplo, problemas de red, o si la API no está disponible), 
    se debe capturar la excepción y manejarla adecuadamente.

    Comportamiento:
    La función toma la frase proporcionada como entrada y la pasa a un servicio de traducción, como puede ser 
    un traductor basado en una API. La traducción resultante se devuelve como un string.

    Ejemplo:
    >>> translate_phrase("Hello, how are you?")
    "Hola, ¿cómo estás?"
    >>> # La frase en inglés se traduce al español.
    """
    traduccion = translator.translate(frase)
    return traduccion


def data_translate(data, columns, table_structure):

    """
    Traduce los datos de las columnas especificadas en una tabla, usando un traductor automatizado para las entradas de texto.

    La función toma los datos de una tabla en formato de lista de listas (donde cada sublista representa una fila de la tabla)
    y traduce los valores de las columnas especificadas a un idioma de destino. Los valores a traducir se toman de las columnas 
    indicadas en la lista `columns` y se procesan para obtener su traducción.

    Parámetros:
    data (list of list): Lista de listas donde cada sublista representa una fila de datos.
    columns (list of str): Lista de nombres de columnas cuyas celdas deben ser traducidas.

    Retorno:
    list of list: Los datos con las celdas de las columnas especificadas traducidas.

    Excepciones:
    Si no se pueden encontrar las columnas en la estructura de la tabla o si ocurre algún error durante la traducción, 
    se podría generar una excepción (aunque no se captura explícitamente en esta implementación).

    Comportamiento:
    - La función primero obtiene los valores de las columnas indicadas.
    - Luego, elimina los valores duplicados para reducir la cantidad de solicitudes de traducción.
    - Los valores únicos se traducen en grupos pequeños (de tamaño 10 en este caso) para optimizar el proceso.
    - Después de obtener las traducciones, las asigna a las celdas correspondientes en las filas originales.

    Ejemplo de Uso:
    >>> data = [
    >>>     ["John", "Doe", "USA"],
    >>>     ["Jane", "Smith", "UK"],
    >>> ]
    >>> columns = ["country"]
    >>> table_structure = ["first_name", "last_name", "country"]
    >>> translated_data = data_translate(data, columns, table_structure)
    >>> # Ahora, la columna "country" tendrá los valores traducidos.
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
