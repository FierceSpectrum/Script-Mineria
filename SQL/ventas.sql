-- Active: 1726878131637@@127.0.0.1@1433@staging
CREATE OR ALTER VIEW fact_ventas AS
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

SELECT * FROM fact_ventas;