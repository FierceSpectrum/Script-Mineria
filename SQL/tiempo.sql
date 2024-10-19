
CREATE OR ALTER VIEW DimFecha AS
SELECT [Fecha_ID]
,[Dia]
,[Mes]
,[Annio]
,[Trimestre]
FROM (
SELECT DISTINCT
    CAST(O.OrderDate AS DATE) AS Fecha_ID,
    DAY(O.OrderDate) AS Dia,
    MONTH(O.OrderDate) AS Mes,
    YEAR(O.OrderDate) AS Annio,
    CASE
        WHEN MONTH(O.OrderDate) IN (1, 2, 3) THEN 1
        WHEN MONTH(O.OrderDate) IN (4, 5, 6) THEN 2
        WHEN MONTH(O.OrderDate) IN (7, 8, 9) THEN 3
        ELSE 4
    END AS Trimestre
FROM
    dbo.Orders AS O

UNION

SELECT DISTINCT
    CAST(J.FECHA_PEDIDO AS DATE) AS Fecha_ID,
    DAY(J.FECHA_PEDIDO) AS Dia,
    MONTH(J.FECHA_PEDIDO) AS Mes,
    YEAR(J.FECHA_PEDIDO) AS Annio,
    CASE
        WHEN MONTH(J.FECHA_PEDIDO) IN (1, 2, 3) THEN 1
        WHEN MONTH(J.FECHA_PEDIDO) IN (4, 5, 6) THEN 2
        WHEN MONTH(J.FECHA_PEDIDO) IN (7, 8, 9) THEN 3
        ELSE 4
    END AS Trimestre
FROM
    dbo.Pedido AS J
) AS V;

SELECT * FROM DimFecha;