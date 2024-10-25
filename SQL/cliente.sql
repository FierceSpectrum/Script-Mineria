-- Active: 1726878131637@@127.0.0.1@1433@staging
CREATE OR ALTER VIEW dim_customers AS
SELECT
    [Customer_ID],
    [Company_Name],
    [Contact_Name],
    [Country],
    [DB_Origin]
FROM (
    SELECT
        CustomerID AS Customer_ID,
        CompanyName AS Company_Name,
        ContactName AS Contact_Name,
        CASE Country
            WHEN 'USA' THEN 'Estados Unidos'
            WHEN 'UK' THEN 'Reino Unido'
            ELSE Country
        END AS Country,
        CAST('Northwind' AS NVARCHAR(10)) AS DB_ORIGIN
    FROM Customers
    
    UNION
    
    SELECT
        CAST(CODIGO_CLIENTE AS varchar(10)) AS Customer_ID,
        NOMBRE_CLIENTE AS Company_Name,
        NOMBRE_CONTACTO AS Contact_Name,
        CASE PAIS
            WHEN 'USA' THEN 'Estados Unidos'
            ELSE PAIS  
        END AS Country,
        'Jardineria' AS DB_Origin  
    FROM Clientes
  ) AS C;
SELECT * FROM dim_customers;
