CREATE OR ALTER VIEW dim_products AS
SELECT
   [product_id],
   [product_name],
   [category], 
   [unit_price],
   [Units_In_Stock],
   [quantity_per_unit],
   [DB_ORIGIN]
FROM
    (
        SELECT 
            CODIGO_PRODUCTO AS [product_id], 
            NOMBRE AS [product_name],
            GAMA AS [category],
            TRY_CAST(PRECIO_VENTA AS DECIMAL(10,2)) AS [unit_price],
            CAST(cantidad_en_STOCK AS VARCHAR(50)) AS [Units_In_Stock], 
            'Unidad única' AS [quantity_per_unit] ,
			cast('JARDINERIA' as nvarchar(10)) AS DB_ORIGIN
        FROM dbo.PRODUCTO
        UNION ALL
        SELECT 
            CAST(ProductID AS VARCHAR(10)) AS [product_id], 
            ProductName AS [product_name], 
            CategoryName AS [category],   
            TRY_CAST(UnitPrice AS DECIMAL(10,2)) AS [unit_price],
            CAST(UnitsInStock AS VARCHAR(50)) AS [Units_In_Stock], 
            QuantityPerUnit AS [quantity_per_unit],'NORTHWIND' AS DB_ORIGIN
        FROM dbo.PRODUCTS AS p
        JOIN dbo.CATEGORIES AS c ON p.CategoryID = c.CategoryID
    ) AS d;




SELECT * FROM dim_products;