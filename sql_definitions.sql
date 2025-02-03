-- Configuration
USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE dbt_wh WITH warehouse_size = 'x-small';
CREATE DATABASE dbt_db;
CREATE ROLE dbt_role;

SHOW GRANTS ON WAREHOUSE dbt_wh;

grant usage on warehouse dbt_wh to role dbt_role;

grant role dbt_role to user jpolanco;
grant all on database dbt_db to role dbt_role;


show grants on database dbt_db;

USE ROLE dbt_role;

create schema dbt_db.dbt_schema;

------------------------------------------------------- SQL QUESION 1 -------------------------------------------------------

-- Create employees table
CREATE OR REPLACE TABLE dbt_db.dbt_schema.employees (
    EmployeeID INT PRIMARY KEY,
    Name VARCHAR(100),
    ManagerID INT
);

-- 3. Insert test data (important for test the query result and set validations)
INSERT INTO dbt_db.dbt_schema.employees (EmployeeID, Name, ManagerID) VALUES
    (1, 'Alice (CEO)', NULL),
    (2, 'Bob (VP of Sales)', 1),
    (3, 'Carol (VP of Engineering)', 1),
    (4, 'David (Sales Manager 1)', 2),
    (5, 'Eve (Sales Manager 2)', 2),
    (6, 'Frank (Engineering Manager 1)', 3),
    (7, 'Grace (Engineering Manager 2)', 3),
    (8, 'Henry (Software Engineer 1)', 6),
    (9, 'Ian (Software Engineer 2)', 6),
    (10, 'Jack (Software Engineer 3)', 7);

-- explore the data
select * from dbt_db.dbt_schema.employees;


--First approach:

--Recursive
-- You can query the result and reuse it
-- Show all Hierarchy
-- Resilient
WITH RECURSIVE Employee_Hierarchy_Tab AS (
    SELECT 
        EmployeeID,
        Name,
        ManagerID,
        0 AS Level,
        EmployeeID AS RootEmployeeID
    FROM dbt_db.dbt_schema.employees
    
    UNION ALL
    
    SELECT 
        e.EmployeeID,
        e.Name,
        e.ManagerID,
        eh.Level + 1 AS Level,
        eh.RootEmployeeID
    FROM dbt_db.dbt_schema.employees e
    INNER JOIN Employee_Hierarchy_Tab eh 
      ON e.EmployeeID = eh.ManagerID
)
SELECT 
    eht.RootEmployeeID AS Employee_ID,
    e.name AS Employee_Name,
    eht.EmployeeID AS Hierarchy_Employee_ID,
    eht.Name AS Hierarchy_Employee_Name,
    eht.Level AS Hierarchy_Level
FROM Employee_Hierarchy_Tab eht
INNER JOIN dbt_db.dbt_schema.employees e 
  ON eht.RootEmployeeID = e.EmployeeID
ORDER BY eht.RootEmployeeID, eht.Level DESC;



-- Seccond approach (Just for testing) this is not the best approach because:
-- you can't reuse the query
-- ineficient by the concatenations
-- if there is inconsistent data, will fail (incomplete hierarchy's or something like that)

WITH RECURSIVE EmployeeHierarchy AS (
    SELECT 
        EmployeeID,
        Name AS Employee_Name,
        ManagerID,
        Name AS ManagerChain,
        1 AS Hierarchy_Level
    FROM dbt_db.dbt_schema.employees

    UNION ALL

    -- Recursive step
    SELECT 
        eh.EmployeeID,
        eh.Employee_Name,
        e.ManagerID,
        eh.ManagerChain || ' → ' || e.Name,  -- Concatena el manager
        eh.Hierarchy_Level + 1
    FROM EmployeeHierarchy eh
    JOIN dbt_db.dbt_schema.employees e ON eh.ManagerID = e.EmployeeID  -- Subir a manager
)
SELECT 
    EmployeeID as employee_id,
    Employee_Name,
    ManagerChain AS Full_Manager_Hierarchy,
    Hierarchy_Level
FROM EmployeeHierarchy
WHERE ManagerID IS NULL  -- Filtra solo las cadenas completas (hasta el CEO)
ORDER BY EmployeeID, Hierarchy_Level DESC;




------------------------------------------------------- SQL QUESION 2 -------------------------------------------------------

--I'm not going to create a dataset for this :) so I'll just write the query.

-- This works even if p.CustomerID IS NULL
-- I suggest to use a better notation for this case (snake_case)

SELECT o.CustomerID as customer_id
FROM Orders o
WHERE NOT EXISTS ( -- customers that are in orders but not in payments table
    SELECT 1 -- create an index
    FROM Payments p
    WHERE p.CustomerID = o.CustomerID
);



------------------------------------------------------ SNOWFLAKE STAR MODEL IMPLEMENTATION -------------------------------

--For this implementation, I will suggest this:
-- use snake_case for have a better readability
-- Do not use reserved words like 'Date'

/*
Fact Table: Tracks sales transactions (TransactionID, Date, CustomerID, ProductID, Amount).
Dimension Tables:
Customers (CustomerID, Name, Email, Country).
Products (ProductID, ProductName, Category, Price).
Dates (Date, Year, Month, Day).

*/

-- Fact Table
CREATE OR REPLACE TABLE dbt_db.dbt_schema.fact_table (
    transaction_id INTEGER PRIMARY KEY,
    transaction_date DATE NOT NULL,
    customer_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    amount DECIMAL(10, 2) NOT NULL
);

-- Customers Dim
CREATE OR REPLACE TABLE dbt_db.dbt_schema.customers (
    customer_id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255),
    country VARCHAR(50)
);


-- Products Dim 
CREATE OR REPLACE TABLE dbt_db.dbt_schema.products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    price DECIMAL(10, 2) NOT NULL
);

-- Date Dim
CREATE OR REPLACE TABLE dbt_db.dbt_schema.dates (
    date_key DATE PRIMARY KEY,
    year_num INTEGER NOT NULL,
    month_num INTEGER NOT NULL, --month number (lets supose is a number and not the name)
    day_num INTEGER NOT NULL --day number (lets supose is a number and not the name)
);


