
# conexion_singleton.py

import conexiones.conexiones as conn

class SingletonConnection:
    _instances = {}

    def __new__(cls, type=None, db_name=None, **kwargs):
        db_key = db_name.lower() if db_name else None

        if db_key not in cls._instances:
            # Crear la conexión solo si no existe una instancia previa para la base de datos especificada
            if type == "oracle":
                cls._instances[db_key] = conn.ora_conndb()
            elif type == "sqlserver":
                cls._instances[db_key] = conn.mssql_conndb(**kwargs)
            else:
                raise ValueError(f"Base de datos '{type}' no está soportada")
        return cls._instances[db_key]

# Inicialización de conexiones para distintos archivos que las necesiten
oradbconn = SingletonConnection("oracle", "Jardineria")
target_conn = SingletonConnection("sqlserver", "Staging", DB_DATABASE="DB_DATABASE", method=1)
target_conn2 = SingletonConnection("sqlserver", "Northwind", DB_DATABASE="DB_DATABASE2")
target_conn3 = SingletonConnection("sqlserver", "WareHouse", DB_DATABASE="DB_DATABASE3")

