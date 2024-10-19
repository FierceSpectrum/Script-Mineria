CREATE OR ALTER VIEW FactVentas AS
SELECT [VentaID]
,[ProductoID]
,[ClienteID]
,[EmpleadoID]
,[FechaID]
,[Cantidad]
,[PrecioUnitario]
,[Descuento]
FROM (
SELECT
    O.OrderID AS VentaID,
    CAST(OD.ProductID AS varchar(10)) AS ProductoID,
    O.CustomerID AS ClienteID,
    O.EmployeeID AS EmpleadoID,
    CAST(O.ShippedDate AS DATE) AS FechaID,
    OD.Quantity AS Cantidad,
    OD.UnitPrice AS PrecioUnitario,
    OD.Discount AS Descuento
FROM
    dbo.[ORDER DETAILS] AS OD
JOIN
    dbo.Orders AS O ON OD.OrderID = O.OrderID
WHERE O.ShippedDate IS NOT NULL

UNION

SELECT
    J.CODIGO_PEDIDO AS VentaID,
    DP.CODIGO_PRODUCTO AS ProductoID,
    CAST(J.CODIGO_CLIENTE AS varchar(10)) AS ClienteID,
    C.CODIGO_EMPLEADO_REP_VENTAS AS EmpleadoID,
    CAST(J.FECHA_ENTREGA AS DATE) AS FechaID,
    DP.Cantidad AS Cantidad,
    DP.PRECIO_UNIDAD AS PrecioUnitario,
    0 AS Descuento 
FROM
    dbo.DETALLE_PEDIDO AS DP
JOIN
    dbo.Pedido AS J ON DP.CODIGO_PEDIDO = J.CODIGO_PEDIDO
JOIN dbo.CLIENTE AS C ON J.CODIGO_CLIENTE = C.CODIGO_CLIENTE

WHERE J.FECHA_ENTREGA IS NOT NULL
) AS V;

SELECT * FROM FactVentas;


CREATE OR ALTER VIEW FactPagos AS
SELECT
    -- P.PagoID AS PagoID,
    P.CODIGO_CLIENTE AS ClienteID,
    CAST(P.FECHA_PAGO AS DATE) AS FechaID,
    P.TOTAL AS TotalPago,
    P.FORMA_PAGO AS FormaPago
FROM
    dbo.Pago AS P;

SELECT * FROM FactPagos;


