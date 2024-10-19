CREATE OR ALTER VIEW PRODUCT AS
SELECT
   [product_id],
    upper(product_name)as[product_name],
    upper(category)as[category],
    [unit_price]
FROM
    (         
        SELECT CODIGO_PRODUCTO as [product_id], NOMBRE as [product_name],
		CASE
			WHEN GAMA = 'Aromáticas' THEN 'Aromatic Plants'
			WHEN GAMA = 'Frutales' THEN 'Fruit Trees'
			WHEN GAMA = 'Herramientas' THEN 'Tools'
			WHEN GAMA = 'Ornamentales' THEN 'Ornamental Plants'
			ELSE GAMA 
		END as [category],
		PRECIO_VENTA as [unit_price] from dbo.PRODUCTO
        UNION
        SELECT cast(ProductID as varchar(10)) ,ProductName,CategoryName,UnitPrice  from dbo.PRODUCTS as p
        JOIN dbo.CATEGORIES as c  ON p.CategoryID = c.CategoryID 
     ) as d;
    