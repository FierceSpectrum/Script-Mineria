-- Active: 1728592622220@@127.0.0.1@1433@staging
CREATE OR ALTER VIEW FACT_SALES AS
SELECT
    [Venta_KEY],
    [PRODUCT_KEY],
    [CUSTOMER_KEY],
    [EMPLOYEE_KEY],
    [DATE_KEY],
    [QUANTITY],
    [UNIT_PRICE],
    [DISCOUNT],
    [DB_ORIGIN]
FROM (
        SELECT
            O.OrderID AS Venta_KEY, CAST(OD.ProductID AS varchar(10)) AS PRODUCT_KEY, O.CustomerID AS CUSTOMER_KEY, O.EmployeeID AS EMPLOYEE_KEY, CASE SUBSTRING(
                    CONVERT(VARCHAR(10), O.OrderDate, 23), 1, 4
                )
                WHEN '1996' THEN CONCAT(
                    '2020-', SUBSTRING(
                        CONVERT(VARCHAR(10), O.OrderDate, 23), 6, 5
                    )
                )
                WHEN '1997' THEN CONCAT(
                    '2021-', SUBSTRING(
                        CONVERT(VARCHAR(10), O.OrderDate, 23), 6, 5
                    )
                )
                WHEN '1998' THEN CONCAT(
                    '2022-', SUBSTRING(
                        CONVERT(VARCHAR(10), O.OrderDate, 23), 6, 5
                    )
                )
                ELSE SUBSTRING(
                    CONVERT(VARCHAR(10), O.OrderDate, 23), 1, 4
                )
            END AS DATE_KEY, OD.Quantity AS QUANTITY, OD.UnitPrice AS UNIT_PRICE, OD.Discount AS DISCOUNT, CAST('NORTHWIND' AS NVARCHAR(10)) AS DB_ORIGIN
        FROM dbo.[ORDER DETAILS] AS OD
            JOIN dbo.Orders AS O ON OD.OrderID = O.OrderID
        UNION
        SELECT
            J.CODIGO_PEDIDO AS Venta_KEY, DP.CODIGO_PRODUCTO AS PRODUCT_KEY, CAST(
                J.CODIGO_CLIENTE AS varchar(10)
            ) AS CUSTOMER_KEY, C.CODIGO_EMPLEADO_REP_VENTAS AS EMPLOYEE_KEY, CONVERT(
                VARCHAR(10), J.FECHA_PEDIDO, 23
            ) AS DATE_KEY, DP.Cantidad AS QUANTITY, DP.PRECIO_UNIDAD AS UNIT_PRICE, 0 AS DISCOUNT, 'JARDINERIA' AS DB_ORIGIN
        FROM dbo.DETALLE_PEDIDO AS DP
            JOIN dbo.Pedido AS J ON DP.CODIGO_PEDIDO = J.CODIGO_PEDIDO
            JOIN dbo.CLIENTE AS C ON J.CODIGO_CLIENTE = C.CODIGO_CLIENTE
        WHERE
            J.ESTADO != 'Rechazado'
    ) AS V;

SELECT * FROM FACT_SALES;

SELECT DISTINCT date_key FROM FACT_SALES;

CREATE OR ALTER VIEW FactPagos AS
SELECT
    -- P.PagoID AS PagoID,
    P.CODIGO_CLIENTE AS ClienteID,
    CAST(P.FECHA_PAGO AS DATE) AS FechaID,
    P.TOTAL AS TotalPago,
    P.FORMA_PAGO AS FormaPago
FROM dbo.Pago AS P;

SELECT * FROM FactPagos;

select * from pedido;

SELECT * FROM detalle_pedido;

SELECT C.codigo_cliente, C.nombre_cliente, PG.total
FROM cliente as C
    inner join pago as PG on C.codigo_cliente = PG.codigo_cliente
    -- inner join pedido as P on c.codigo_cliente = P.codigo_cliente
    -- inner join

SELECT C.codigo_cliente, C.nombre_cliente, P.codigo_pedido, P.estado, (
        DP.cantidad * DP.precio_unidad
    ) as pagar
FROM
    cliente as C
    inner join pedido as P on C.codigo_cliente = P.codigo_cliente
    inner join detalle_pedido as DP on P.codigo_pedido = DP.codigo_pedido
where
    p.estado = 'Entregado'
order by c.codigo_cliente;

SELECT
    OrderDate,
    RequiredDate,
    ShippedDate
FROM ORDERS
WHERE
    OrderDate IS null
    OR RequiredDate IS null
    OR ShippedDate IS null;