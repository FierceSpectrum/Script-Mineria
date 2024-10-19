CREATE OR ALTER VIEW INFO_CLIENTES AS
    SELECT CustomerID AS Codigo_Cliente, CompanyName AS Nombre_Empresa, ContactName AS Nombre_Contacto, Country AS Pais
FROM CustomersUNION
    SELECT CAST(CODIGO_CLIENTE AS varchar(10)) AS Codigo_Cliente, NOMBRE_CLIENTE AS Nombre_Empresa, NOMBRE_CONTACTO AS Nombre_Contacto, PAIS AS Pais
FROM Clientes;


SELECT * FROM INFO_CLIENTES;
