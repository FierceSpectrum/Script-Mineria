-- Active: 1726878131637@@127.0.0.1@1433@staging
CREATE OR ALTER VIEW dim_employes AS
SELECT 
    [Codigo_Cliente],
    [Nombre_Empresa],
    [Nombre_Contacto],
    [Pais]
FROM (
    SELECT CustomerID AS Codigo_Cliente, 
    CompanyName AS Nombre_Empresa, 
    ContactName AS Nombre_Contacto, 
    Country AS Pais
    FROM Customers
    UNION
    SELECT CAST(CODIGO_CLIENTE AS varchar(10)) AS Codigo_Cliente, 
    NOMBRE_CLIENTE AS Nombre_Empresa, 
    NOMBRE_CONTACTO AS Nombre_Contacto, 
    PAIS AS Pais
    FROM Cliente
) AS V;

SELECT * FROM dim_employes;
