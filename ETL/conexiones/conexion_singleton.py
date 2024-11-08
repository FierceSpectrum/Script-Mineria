"""
Módulo de Conexión Singleton para Bases de Datos (SQL Server y Oracle)

Este módulo implementa un patrón Singleton para gestionar conexiones únicas a 
bases de datos. Permite instanciar una única conexión a cada base de datos 
específica (por ejemplo, Oracle o SQL Server) y reutilizarla en otros módulos 
que la necesiten.

Funcionalidades principales:
- `SingletonConnection`: Clase que implementa el patrón Singleton para crear 
  y mantener una única instancia de conexión para cada base de datos especificada.
- `oradbconn`, `target_conn`, `target_conn2`, `target_conn3`: Variables de 
  conexión predefinidas para bases de datos específicas, que pueden ser utilizadas 
  en otros módulos del proyecto.

Uso:
Este módulo está diseñado para ser importado en otros archivos que requieran 
conexiones de bases de datos. Utilizando `SingletonConnection`, cada base de 
datos especificada tendrá una única conexión compartida en todo el proyecto.

Requerimientos:
- Módulo `conexiones.conexiones`, que contiene las funciones de conexión `ora_conndb` 
  y `mssql_conndb` para Oracle y SQL Server, respectivamente.
"""

# Importación del módulo de conexiones
import conexiones.conexiones as conn


class SingletonConnection:
    """
    Clase SingletonConnection para gestionar conexiones únicas a bases de datos.

    Esta clase permite crear y mantener una única conexión para cada base de datos 
    especificada (Oracle o SQL Server) usando un patrón Singleton. La conexión es 
    almacenada en un diccionario `_instances` para que pueda reutilizarse en cada 
    solicitud.

    Métodos:
    - __new__: Sobrecarga el método `__new__` para controlar la creación de instancias.
    """
    _instances = {}

    def __new__(cls, type=None, db_name=None, **kwargs):
        """
        Crea o reutiliza una conexión a base de datos según el tipo y el nombre.

        Parámetros:
        - type (str): Tipo de base de datos (por ejemplo, "oracle" o "sqlserver").
        - db_name (str): Nombre de la base de datos para identificar la conexión.
        - kwargs: Argumentos adicionales que se pasan a las funciones de conexión.

        Retorna:
        - Una instancia de conexión específica para la base de datos solicitada.

        Excepciones:
        - Levanta un `ValueError` si el tipo de base de datos no está soportado.
        """
        
        db_key = db_name.lower() if db_name else None

        # Verifica si la conexión ya existe en `_instances`; si no, la crea
        if db_key not in cls._instances:
            # Establece la conexión según el tipo de base de datos
            if type == "oracle":
                cls._instances[db_key] = conn.ora_conndb()
            elif type == "sqlserver":
                cls._instances[db_key] = conn.mssql_conndb(**kwargs)
            else:
                raise ValueError(f"Base de datos '{type}' no está soportada")

        return cls._instances[db_key]


# Inicialización de conexiones para distintos archivos que las necesiten

oradbconn = SingletonConnection("oracle", "Jardineria")  # Conexión a Oracle

# Conexión a SQL Server (Staging)
target_conn = SingletonConnection(
    "sqlserver", "Staging", DB_DATABASE="DB_DATABASE", method=1)

# Conexión a SQL Server (Northwind)
target_conn2 = SingletonConnection(
    "sqlserver", "Northwind", DB_DATABASE="DB_DATABASE2")

# Conexión a SQL Server (WareHouse)
target_conn3 = SingletonConnection(
    "sqlserver", "WareHouse", DB_DATABASE="DB_DATABASE3")
