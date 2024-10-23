-- Create dimension
CREATE TABLE dim_employees (
    employee_key INT IDENTITY(1,1) CONSTRAINT pk_employees PRIMARY KEY,
    employee_id nvarchar(10),
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

CREATE OR ALTER VIEW vdim_employees AS
SELECT 
	[employee_id],
	[full_name],
	[title],
	[city],
	[country],
	[reports_to]
FROM (
	SELECT 
		CAST(EM.CODIGO_EMPLEADO AS NVARCHAR(10)) AS EMPLOYEE_ID, 
		EM.NOMBRE + ' ' + EM.APELLIDO1 AS FULL_NAME, 
		EM.PUESTO AS TITLE, 
		UPPER(O.CIUDAD) AS CITY,
		UPPER(
			CASE 
				WHEN O.PAIS = 'EEUU' THEN 'ESTADOS UNIDOS'
			ELSE O.PAIS
			END
		) AS COUNTRY, 
		ISNULL(J.NOMBRE + ' ' + J.APELLIDO1, 'COMITE GERENCIAL') AS REPORTS_TO
	FROM EMPLEADO EM
	LEFT JOIN OFICINA O ON EM.CODIGO_OFICINA = O.CODIGO_OFICINA
	LEFT JOIN EMPLEADO J ON EM.CODIGO_JEFE = J.CODIGO_EMPLEADO
	
	UNION
	
	SELECT 
		E.EmployeeID AS EMPLOYEE_ID, 
		E.FirstName + ' ' + E.LastName AS FULL_NAME, 
		E.Title AS TITLE, 
		UPPER(E.City) AS CITY,
		UPPER(
			CASE 
				WHEN E.Country = 'USA' THEN 'UNITED STATES'
				WHEN E.Country = 'EEUU' THEN 'ESTADOS UNIDOS'
				WHEN E.Country = 'UK' THEN 'UNITED KINGDOM'
			ELSE E.Country
			END
		) AS COUNTRY,  
		ISNULL(B.FirstName + ' ' + B.LastName, 'COMITE GERENCIAL') AS REPORTS_TO
	FROM EMPLOYEES E
	LEFT JOIN EMPLOYEES B ON E.ReportsTo = B.EmployeeID
) AS EMPLOYEES_DATA;