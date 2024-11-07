CREATE OR ALTER VIEW DimCliente AS
SELECT [Cliente_ID]
,[Nombre_Cliente]
,[Contacto]
,[Telefono]
,[Direccion]
,[Ciudad]
,[Region]
,[Codigo_Postal]
,[Pais]
FROM ( 
SELECT
    C.CustomerID AS Cliente_ID,
    C.CompanyName AS Nombre_Cliente,
    C.ContactName AS Contacto,
    C.Phone AS Telefono,
    C.Address AS Direccion,
    C.City AS Ciudad,
    C.Region AS Region,
    C.PostalCode AS Codigo_Postal,
    C.Country AS Pais
FROM
    dbo.Customers AS C


UNION

SELECT
    CAST(J.CODIGO_CLIENTE AS varchar(10)) AS Cliente_ID,
    J.NOMBRE_CLIENTE AS Nombre_Cliente,
    J.NOMBRE_CONTACTO AS Contacto,
    J.TELEFONO AS Telefono,
    J.LINEA_DIRECCION1 AS Direccion,
    J.CIUDAD AS Ciudad,
    J.REGION AS Region,
    J.CODIGO_POSTAL AS Codigo_Postal,
    J.Pais AS Pais
FROM
    dbo.Cliente AS J
) AS V;

SELECT * FROM DimCliente;

CREATE OR ALTER VIEW DimProducto AS
SELECT [Producto_ID]
,[Nombre_Producto]
,[Categoria]
,[Proveedor]
,[Precio_Venta]
,[Stock]
FROM (
SELECT
    CAST(P.ProductID AS varchar(10)) AS Producto_ID,
    P.ProductName AS Nombre_Producto,
    C.CategoryName AS Categoria,
    S.CompanyName AS Proveedor,
    P.UnitPrice AS Precio_Venta,
    P.UnitsInStock AS Stock
FROM
    dbo.Products AS P
JOIN
    dbo.Categories AS C ON P.CategoryID = C.CategoryID
JOIN
    dbo.Suppliers AS S ON P.SupplierID = S.SupplierID


UNION

SELECT
    J.CODIGO_PRODUCTO AS Producto_ID,
    J.NOMBRE AS Nombre_Producto,
    J.GAMA AS Categoria,
    J.PROVEEDOR AS Proveedor,
    J.PRECIO_VENTA AS Precio_Venta,
    J.CANTIDAD_EN_STOCK AS Stock
FROM
    dbo.Producto AS J
) AS V;

SELECT * FROM DimProducto;

CREATE OR ALTER VIEW DimEmpleado AS
SELECT [Empleado_ID]
,[Nombre_Empleado]
,[Puesto]
,[Fecha_Contratacion]
,[Region]
FROM (
SELECT
    E.EmployeeID AS Empleado_ID,
    E.LastName + ', ' + E.FirstName AS Nombre_Empleado,
    E.Title AS Puesto,
    E.HireDate AS Fecha_Contratacion,
    E.Region AS Region
FROM
    dbo.Employees AS E


UNION

SELECT
    J.CODIGO_EMPLEADO AS Empleado_ID,
    J.APELLIDO1 + ', ' + J.NOMBRE AS Nombre_Empleado,
    J.Puesto AS Puesto,
    NULL AS Fecha_Contratacion,
    O.REGION AS Region
FROM
    dbo.Empleado AS J
JOIN
    dbo.Oficina AS O ON J.CODIGO_OFICINA = O.CODIGO_OFICINA
) AS V;

SELECT * FROM DimEmpleado;

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

"CREATE TABLE 'FACT_SALES'( 'SALE_KEY' int IDENTITY(1,1) NOT NULL, 'VENTA_KEY' int, 'PRODUCT_KEY' int, 'CUSTOMER_KEY' int, 'EMPLOYEE_KEY' int, 'DATE_KEY' int, 'QUANTITY' int, 'UNIT_PRICE' decimal, 'DISCOUNT' real, 'DB_ORIGIN' nvarchar(10), PRIMARY KEY(SALE_KEY), CONSTRAINT FK_SALES_EMPLOYEES FOREIGN key('EMPLOYEE_KEY') REFERENCES 'VDIM_EMPLOYEES'('EMPLOYEE_KEY'), CONSTRAINT FK_SALES_DATES FOREIGN key('DATE_KEY') REFERENCES 'VDIM_DATES'('DATE_KEY'), CONSTRAINT FK_SALES_PRODUCTS FOREIGN key('PRODUCT_KEY') REFERENCES 'VDIM_PRODUCTS'('PRODUCT_KEY'), CONSTRAINT FK_SALES_CUSTOMERS FOREIGN key('CUSTOMER_KEY') REFERENCES 'VDIM_CUSTOMERS'('CUSTOMER_KEY') );"

CREATE TABLE "VDIM_CUSTOMERS" (
    "CUSTOMER_KEY" int IDENTITY(1, 1) NOT NULL,
    "CUSTOMER_ID" nvarchar(10),
    "COMPANY_NAME" nvarchar(50),
    "CONTACT_NAME" nvarchar(30),
    "COUNTRY" nvarchar(50),
    "DB_ORIGIN" nvarchar(10),
    PRIMARY KEY (CUSTOMER_KEY)
);

CREATE TABLE "VDIM_DATES" (
    "DATE_KEY" int IDENTITY(1, 1) NOT NULL,
    "DATE_ID" date,
    "YEAR" int,
    "MOUNTH" int,
    "DAY" int,
    "FOUR_MONTH" int,
    "QUARTER" int,
    "WEEK" int,
    "WEEK_DAY" varchar(9),
    "YEAR_DAY" int,
    PRIMARY KEY (DATE_KEY)
);

CREATE TABLE "VDIM_EMPLOYEES" (
    "EMPLOYEE_KEY" int IDENTITY(1, 1) NOT NULL,
    "EMPLOYEE_ID" int,
    "FULL_NAME" nvarchar(101),
    "TITLE" nvarchar(50),
    "CITY" nvarchar(30),
    "COUNTRY" nvarchar(50),
    "REPORTS_TO" nvarchar(101),
    "DB_ORIGIN" nvarchar(10),
    PRIMARY KEY (EMPLOYEE_KEY)
);

CREATE TABLE "VDIM_PRODUCTS" (
    "PRODUCT_KEY" int IDENTITY(1, 1) NOT NULL,
    "PRODUCT_ID" nvarchar(15),
    "PRODUCT_NAME" nvarchar(70),
    "CATEGORY" nvarchar(50),
    "UNIT_PRICE" decimal,
    "UNITS_IN_STOCK" varchar(50),
    "QUANTITY_PER_UNIT" nvarchar(100),
    "DB_ORIGIN" nvarchar(10),
    PRIMARY KEY (PRODUCT_KEY)
);

CREATE TABLE "FACT_SALES" (
    "SALE_KEY" int IDENTITY(1, 1) NOT NULL,
    "VENTA_KEY" int,
    "PRODUCT_KEY" int,
    "CUSTOMER_KEY" int,
    "EMPLOYEE_KEY" int,
    "DATE_KEY" int,
    "QUANTITY" int,
    "UNIT_PRICE" decimal,
    "DISCOUNT" real,
    "DB_ORIGIN" nvarchar(10),
    PRIMARY KEY (SALE_KEY),
    CONSTRAINT FK_SALES_EMPLOYEES FOREIGN key ("EMPLOYEE_KEY") REFERENCES "VDIM_EMPLOYEES" ("EMPLOYEE_KEY"),
    CONSTRAINT FK_SALES_DATES FOREIGN key ("DATE_KEY") REFERENCES "VDIM_DATES" ("DATE_KEY"),
    CONSTRAINT FK_SALES_PRODUCTS FOREIGN key ("PRODUCT_KEY") REFERENCES "VDIM_PRODUCTS" ("PRODUCT_KEY"),
    CONSTRAINT FK_SALES_CUSTOMERS FOREIGN key ("CUSTOMER_KEY") REFERENCES "VDIM_CUSTOMERS" ("CUSTOMER_KEY")
);