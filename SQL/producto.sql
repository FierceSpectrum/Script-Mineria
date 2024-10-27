-- Active: 1726878131637@@127.0.0.1@1433@staging
CREATE OR ALTER VIEW dim_products AS
SELECT
   [PRODUCT_ID],
   [PRODUCT_NAME],
   [CATEGORY], 
   [UNIT_PRICE],
   [UNITS_IN_STOCK],
   [QUANTITY_PER_UNIT],
   [DB_ORIGIN]
FROM
    (
        SELECT 
            CODIGO_PRODUCTO AS [PRODUCT_ID], 
            NOMBRE AS [PRODUCT_NAME],
            GAMA AS [CATEGORY],
            TRY_CAST(PRECIO_VENTA AS DECIMAL(10,2)) AS [UNIT_PRICE],
            CAST(cantidad_en_STOCK AS VARCHAR(50)) AS [UNITS_IN_STOCK], 
            CAST('Unidad unica' AS NVARCHAR(50)) AS [QUANTITY_PER_UNIT],
			cast('JARDINERIA' as nvarchar(10)) AS DB_ORIGIN
        FROM dbo.PRODUCTO
        UNION ALL
        SELECT 
            CAST(ProductID AS VARCHAR(10)) AS [PRODUCT_ID], 
            ProductName AS [PRODUCT_NAME], 
            CategoryName AS [CATEGORY],   
            TRY_CAST(UnitPrice AS DECIMAL(10,2)) AS [UNIT_PRICE],
            CAST(UnitsInStock AS VARCHAR(50)) AS [UNITS_IN_STOCK], 
            QuantityPerUnit AS [quantity_per_unit],'NORTHWIND' AS DB_ORIGIN
        FROM dbo.PRODUCTS AS p
        JOIN dbo.CATEGORIES AS c ON p.CategoryID = c.CategoryID
    ) AS d;




SELECT * FROM dim_products;