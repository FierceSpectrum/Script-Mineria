-- Active: 1728592622220@@127.0.0.1@1433@staging
CREATE OR ALTER VIEW VDIM_DATES AS
SELECT
    [DATE],
    [YEAR],
    [MOUNTH],
    [DAY],
    [FOUR_MONTH],
    [QUARTER],
    [WEEK],
    [WEEK_DAY],
    [YEAR_DAY],
    [DB_ORIGIN]
FROM (
        SELECT DISTINCT
            CASE SUBSTRING(
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
            END AS DATE, CASE YEAR(O.OrderDate)
                WHEN '1996' THEN '2020'
                WHEN '1997' THEN '2021'
                WHEN '1998' THEN '2022'
                ELSE YEAR(O.OrderDate)
            END AS YEAR, MONTH(O.OrderDate) AS MOUNTH, DAY(O.OrderDate) AS DAY, CASE
                WHEN MONTH(O.OrderDate) IN (1, 2, 3) THEN 1
                WHEN MONTH(O.OrderDate) IN (4, 5, 6) THEN 2
                WHEN MONTH(O.OrderDate) IN (7, 8, 9) THEN 3
                ELSE 4
            END AS FOUR_MONTH, CASE
                WHEN MONTH(O.OrderDate) IN (1, 2, 3, 4) THEN 1
                WHEN MONTH(O.OrderDate) IN (5, 6, 7, 8) THEN 2
                ELSE 3
            END AS QUARTER, DATEPART(WEEK, O.OrderDate) AS WEEK, CASE DATEPART(WEEKDAY, O.OrderDate)
                WHEN 1 THEN 'Sunday'
                WHEN 2 THEN 'Monday'
                WHEN 3 THEN 'Tuesday'
                WHEN 4 THEN 'Wednesday'
                WHEN 5 THEN 'Thursday'
                WHEN 6 THEN 'Friday'
                WHEN 7 THEN 'Saturday'
            END AS WEEK_DAY, DATEPART(DAYOFYEAR, O.OrderDate) AS YEAR_DAY, CAST('NORTHWIND' AS NVARCHAR(10)) AS DB_ORIGIN
        FROM dbo.Orders AS O
        UNION
        SELECT DISTINCT
            CONVERT(
                VARCHAR(10), J.FECHA_PEDIDO, 23
            ) AS DATE, YEAR(J.FECHA_PEDIDO) AS YEAR, MONTH(J.FECHA_PEDIDO) AS MOUNTH, DAY(J.FECHA_PEDIDO) AS DAY, CASE
                WHEN MONTH(J.FECHA_PEDIDO) IN (1, 2, 3) THEN 1
                WHEN MONTH(J.FECHA_PEDIDO) IN (4, 5, 6) THEN 2
                WHEN MONTH(J.FECHA_PEDIDO) IN (7, 8, 9) THEN 3
                ELSE 4
            END AS FOUR_MONTH, CASE
                WHEN MONTH(J.FECHA_PEDIDO) IN (1, 2, 3, 4) THEN 1
                WHEN MONTH(J.FECHA_PEDIDO) IN (5, 6, 7, 8) THEN 2
                ELSE 3
            END AS QUARTER, DATEPART(WEEK, J.FECHA_PEDIDO) AS WEEK, CASE DATEPART(WEEKDAY, J.FECHA_PEDIDO)
                WHEN 1 THEN 'Domingo'
                WHEN 2 THEN 'Lunes'
                WHEN 3 THEN 'Martes'
                WHEN 4 THEN 'Miercoles'
                WHEN 5 THEN 'Jueves'
                WHEN 6 THEN 'Viernes'
                WHEN 7 THEN 'Sabado'
            END AS WEEK_DAY, DATEPART(DAYOFYEAR, J.FECHA_PEDIDO) AS YEAR_DAY, 'JARDINERIA' AS DB_ORIGIN
        FROM dbo.Pedido AS J
    ) AS V;

SELECT * FROM VDIM_DATES;

SELECT DISTINCT CASE SUBSTRING( CONVERT(VARCHAR(10), O.OrderDate, 23), 1, 4 ) WHEN '1996' THEN CONCAT( '2020-', SUBSTRING( CONVERT(VARCHAR(10), O.OrderDate, 23), 6, 5 ) ) WHEN '1997' THEN CONCAT( '2021-', SUBSTRING( CONVERT(VARCHAR(10), O.OrderDate, 23), 6, 5 ) ) WHEN '1998' THEN CONCAT( '2022-', SUBSTRING( CONVERT(VARCHAR(10), O.OrderDate, 23), 6, 5 ) ) ELSE SUBSTRING( CONVERT(VARCHAR(10), O.OrderDate, 23), 1, 4 ) END AS DATE, CASE YEAR(O.OrderDate) WHEN '1996' THEN '2020' WHEN '1997' THEN '2021' WHEN '1998' THEN '2022' ELSE YEAR(O.OrderDate) END AS YEAR, MONTH(O.OrderDate) AS MOUNTH, DAY(O.OrderDate) AS DAY, CASE WHEN MONTH(O.OrderDate) IN (1, 2, 3) THEN 1 WHEN MONTH(O.OrderDate) IN (4, 5, 6) THEN 2 WHEN MONTH(O.OrderDate) IN (7, 8, 9) THEN 3 ELSE 4 END AS FOUR_MONTH, CASE WHEN MONTH(O.OrderDate) IN (1, 2, 3, 4) THEN 1 WHEN MONTH(O.OrderDate) IN (5, 6, 7, 8) THEN 2 ELSE 3 END AS QUARTER, DATEPART(WEEK, O.OrderDate) AS WEEK, CASE DATEPART(WEEKDAY, O.OrderDate) WHEN 1 THEN 'Domingo' WHEN 2 THEN 'Lunes' WHEN 3 THEN 'Martes' WHEN 4 THEN 'Miercoles' WHEN 5 THEN 'Jueves' WHEN 6 THEN 'Viernes' WHEN 7 THEN 'Sabado' END AS WEEK_DAY, DATEPART(DAYOFYEAR, O.OrderDate) AS YEAR_DAY, CAST('NORTHWIND' AS NVARCHAR(10)) AS DB_ORIGIN FROM dbo.Orders AS O UNION SELECT DISTINCT CONVERT( VARCHAR(10), J.FECHA_PEDIDO, 23 ) AS DATE, YEAR(J.FECHA_PEDIDO) AS YEAR, MONTH(J.FECHA_PEDIDO) AS MOUNTH, DAY(J.FECHA_PEDIDO) AS DAY, CASE WHEN MONTH(J.FECHA_PEDIDO) IN (1, 2, 3) THEN 1 WHEN MONTH(J.FECHA_PEDIDO) IN (4, 5, 6) THEN 2 WHEN MONTH(J.FECHA_PEDIDO) IN (7, 8, 9) THEN 3 ELSE 4 END AS FOUR_MONTH, CASE WHEN MONTH(J.FECHA_PEDIDO) IN (1, 2, 3, 4) THEN 1 WHEN MONTH(J.FECHA_PEDIDO) IN (5, 6, 7, 8) THEN 2 ELSE 3 END AS QUARTER, DATEPART(WEEK, J.FECHA_PEDIDO) AS WEEK, CASE DATEPART(WEEKDAY, J.FECHA_PEDIDO) WHEN 1 THEN 'Domingo' WHEN 2 THEN 'Lunes' WHEN 3 THEN 'Martes' WHEN 4 THEN 'Miercoles' WHEN 5 THEN 'Jueves' WHEN 6 THEN 'Viernes' WHEN 7 THEN 'Sabado' END AS WEEK_DAY, DATEPART(DAYOFYEAR, J.FECHA_PEDIDO) AS YEAR_DAY, 'JARDINERIA' AS DB_ORIGIN FROM dbo.Pedido AS J