-- Active: 1730769628889@@127.0.0.1@1433@staging
CREATE
OR
ALTER VIEW VDIM_PRODUCTS AS
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
            PD.CODIGO_PRODUCTO AS PRODUCT_ID,
            PD.NOMBRE AS PRODUCT_NAME,
            PD.GAMA AS CATEGORY,
            TRY_CAST (PD.PRECIO_VENTA AS DECIMAL(10, 2)) AS UNIT_PRICE,
            CAST(PD.CANTIDAD_EN_STOCK AS VARCHAR(50)) AS UNITS_IN_STOCK,
            CAST('Unidad unica' AS NVARCHAR (100)) AS QUANTITY_PER_UNIT,
            cast('JARDINERIA' as nvarchar (10)) AS DB_ORIGIN
        FROM
            dbo.PRODUCTO AS PD
        UNION ALL
        SELECT
            CAST(P.ProductID AS VARCHAR(10)) AS PRODUCT_ID,
            P.ProductName AS PRODUCT_NAME,
            C.CategoryName AS CATEGORY,
            TRY_CAST (P.UnitPrice AS DECIMAL(10, 2)) AS UNIT_PRICE,
            CAST(P.UnitsInStock AS VARCHAR(50)) AS UNITS_IN_STOCK,
            P.QuantityPerUnit AS QUANTITY_PER_UNIT,
            'NORTHWIND' AS DB_ORIGIN
        FROM
            dbo.PRODUCTS AS P
            JOIN dbo.CATEGORIES AS C ON P.CategoryID = C.CategoryID
    ) AS d;

SELECT
    *
FROM
    VDIM_PRODUCTS;