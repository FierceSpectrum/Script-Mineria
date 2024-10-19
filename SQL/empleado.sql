-- Create dimension
CREATE TABLE dim_employees (
    employee_key INT IDENTITY(1,1) CONSTRAINT pk_employees PRIMARY KEY,
    employee_id INT,
    full_name NVARCHAR(80),
    title NVARCHAR(100),
    -- hire_date DATE,
    -- office_code NVARCHAR(20),
    city NVARCHAR(60),
    region NVARCHAR(60),
    country NVARCHAR(60),
    reports_to INT
);


-- Create view to add data in the dimension "dim_employees"

-- CREATE VIEW vdim_employees AS
ALTER VIEW vdim_employees AS
SELECT 
	[employee_id],
	[full_name],
	[title],
	[city],
	[region],
	[country],
	[reports_to]
FROM (
	SELECT 
		EM.CODIGO_EMPLEADO AS EMPLOYEE_ID, 
		EM.NOMBRE + ' ' + EM.APELLIDO1 AS FULL_NAME, 
		EM.PUESTO AS TITLE, 
		O.CIUDAD AS CITY, 
		O.REGION AS REGION, 
		O.PAIS AS COUNTRY, 
		J.NOMBRE + ' ' + J.APELLIDO1 AS REPORTS_TO
	FROM EMPLEADO EM
	LEFT JOIN OFICINA O ON EM.CODIGO_OFICINA = O.CODIGO_OFICINA
	LEFT JOIN EMPLEADO J ON EM.CODIGO_JEFE = J.CODIGO_EMPLEADO
	
	UNION
	
	SELECT 
		E.EmployeeID AS EMPLOYEE_ID, 
		E.FirstName + ' ' + E.LastName AS FULL_NAME, 
		E.Title AS TITLE, 
		E.City AS CITY, 
		E.Region AS REGION, 
		E.Country AS COUNTRY, 
		B.FirstName + ' ' + B.LastName AS REPORTS_TO
	FROM EMPLOYEES E
	LEFT JOIN EMPLOYEES B ON E.ReportsTo = B.EmployeeID
) AS EMPLOYEES_DATA;
