/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DWHAnalytics' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, this script creates a schema called gold
	
WARNING:
    Running this script will drop the entire 'DWHAnalytics' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

USE master;
GO

-- Drop and recreate the 'DWHAnalytics' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DWHAnalytics')
BEGIN
    ALTER DATABASE DWHAnalytics SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DWHAnalytics;
END;
GO

-- Create the 'DWHAnalytics' database
CREATE DATABASE DWHAnalytics;
GO

USE DWHAnalytics;
GO

-- Create Schemas

CREATE SCHEMA gold;
GO

CREATE TABLE gold.dim_customers(
	customer_key int,
	customer_id int,
	customer_number nvarchar(50),
	first_name nvarchar(50),
	last_name nvarchar(50),
	country nvarchar(50),
	marital_status nvarchar(50),
	gender nvarchar(50),
	birthdate date,
	create_date date
);
GO

CREATE TABLE gold.dim_products(
	product_key int ,
	product_id int ,
	product_number nvarchar(50) ,
	product_name nvarchar(50) ,
	category_id nvarchar(50) ,
	category nvarchar(50) ,
	subcategory nvarchar(50) ,
	maintenance nvarchar(50) ,
	cost int,
	product_line nvarchar(50),
	start_date date 
);
GO

CREATE TABLE gold.fact_sales(
	order_number nvarchar(50),
	product_key int,
	customer_key int,
	order_date date,
	shipping_date date,
	due_date date,
	sales_amount int,
	quantity tinyint,
	price int 
);
GO

TRUNCATE TABLE gold.dim_customers;
GO

BULK INSERT gold.dim_customers
FROM 'C:\Users\megvm\OneDrive\Desktop\SQL\Project Files\sql-data-analytics-project\sql-data-analytics-project\datasets\csv-files\gold.dim_customers.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
GO

TRUNCATE TABLE gold.dim_products;
GO

BULK INSERT gold.dim_products
FROM 'C:\Users\megvm\OneDrive\Desktop\SQL\Project Files\sql-data-analytics-project\sql-data-analytics-project\datasets\csv-files\gold.dim_products.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
GO

TRUNCATE TABLE gold.fact_sales;
GO

BULK INSERT gold.fact_sales
FROM 'C:\Users\megvm\OneDrive\Desktop\SQL\Project Files\sql-data-analytics-project\sql-data-analytics-project\datasets\csv-files\gold.fact_sales.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
GO

-- ============================================================
-- Exploratory data analysis --
-- ============================================================

/*
===============================================================================
Dimensions Exploration
===============================================================================
Purpose:
    - To explore the structure of dimension tables.
	
SQL Functions Used:
    - DISTINCT
    - ORDER BY
===============================================================================
*/

-- Retrieve a list of unique countries from which customers originate
SELECT DISTINCT 
    country 
FROM gold.dim_customers
ORDER BY country;

-- Retrieve a list of unique categories, subcategories, and products
SELECT DISTINCT 
    category, 
    subcategory, 
    product_name 
FROM gold.dim_products
ORDER BY category, subcategory, product_name;

-- DATE RANGE EXPLORATION --
/* Purpose:
    - To determine the temporal boundaries of key data points.
    - To understand the range of historical data.

SQL Functions Used:
    - MIN(), MAX(), DATEDIFF()
*/

-- The first and last order date and the total duration in months

SELECT 
	MIN(order_date) AS first_order,
	MAX(order_date) AS recent_order,
	DATEDIFF(MONTH,MIN(order_date),MAX(order_date)) AS duration_in_months
FROM gold.fact_sales

-- the youngest and oldest customer based on birthdate

SELECT 
	MIN(birthdate) AS old_birthdate,
	MAX(birthdate) AS  young_birthdate,
	DATEDIFF(YEAR,MIN(birthdate),GETDATE()) AS oldest_customer_age,
	DATEDIFF(YEAR,MAX(birthdate),GETDATE()) AS youngest_customer_age
FROM gold.dim_customers

/*
===============================================================================
Measures Exploration (Key Metrics)
===============================================================================
Purpose:
    - To calculate aggregated metrics (e.g., totals, averages) for quick insights.
    - To identify overall trends or spot anomalies.

SQL Functions Used:
    - COUNT(), SUM(), AVG()
===============================================================================
*/

-- Total Sales
SELECT SUM(sales_amount) AS total_sales FROM gold.fact_sales

--  how many items are sold
SELECT SUM(quantity) AS total_quantity FROM gold.fact_sales

-- average selling price
SELECT AVG(price) AS avg_price FROM gold.fact_sales

-- Total number of Orders
SELECT COUNT(order_number) AS total_orders FROM gold.fact_sales
SELECT COUNT(DISTINCT order_number) AS total_orders FROM gold.fact_sales

-- total number of products
SELECT COUNT(product_name) AS total_products FROM gold.dim_products

-- total number of customers
SELECT COUNT(customer_key) AS total_customers FROM gold.dim_customers;

-- total number of customers that has placed an order
SELECT COUNT(DISTINCT customer_key) AS total_customers FROM gold.fact_sales;

-- Generate a report that contains all key metrics

SELECT 'total_sales' AS measure_name, SUM(sales_amount) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'quantities_sold' AS measure_name, SUM(quantity) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Average Price', AVG(price) FROM gold.fact_sales
UNION ALL
SELECT 'Total Orders', COUNT(DISTINCT order_number) FROM gold.fact_sales
UNION ALL
SELECT 'Total Products', COUNT(DISTINCT product_name) FROM gold.dim_products
UNION ALL
SELECT 'Total Customers', COUNT(customer_key) FROM gold.dim_customers;

/*
===============================================================================
Magnitude Analysis
===============================================================================
Purpose:
    - To quantify data and group results by specific dimensions.
    - For understanding data distribution across categories.

SQL Functions Used:
    - Aggregate Functions: SUM(), COUNT(), AVG()
    - GROUP BY, ORDER BY
===============================================================================
*/

-- total customers by countries

SELECT 
	country,
	COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY country
ORDER BY total_customers DESC;

-- total customers by gender

SELECT 
	gender,
	COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY gender
ORDER BY total_customers DESC;

-- total products by category

SELECT 
	category,
	COUNT(product_key) AS total_products
FROM gold.dim_products
GROUP BY category
ORDER BY total_products DESC;


-- average costs in each category

SELECT
	category,
	AVG(cost) AS avg_cost
FROM gold.dim_products
GROUP BY category
ORDER BY avg_cost DESC

-- total revenue generated for each category

SELECT
	p.category,
	SUM(s.sales_amount) AS revenue_by_cat
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p
ON s.product_key=p.product_key
GROUP BY p.category
ORDER BY revenue_by_cat DESC

-- total revenue generated by each customer

SELECT TOP 10
	c.customer_id,
	c.first_name,
	c.last_name,
	SUM(s.sales_amount) AS total_revenue
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c
ON s.customer_key=c.customer_key
GROUP BY 
    c.customer_id,
	c.first_name,
	c.last_name
ORDER BY total_revenue DESC

-- distribution of sold items across countries

SELECT
	c.country,
	SUM(s.quantity) AS number_of_solditems
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c
ON s.customer_key = c.customer_key
GROUP BY c.country
ORDER BY number_of_solditems DESC


/*
===============================================================================
Ranking Analysis
===============================================================================
Purpose:
    - To rank items (e.g., products, customers) based on performance or other metrics.
    - To identify top performers or laggards.

SQL Functions Used:
    - Window Ranking Functions: RANK(), DENSE_RANK(), ROW_NUMBER(), TOP
    - Clauses: GROUP BY, ORDER BY
===============================================================================
*/

-- Top 5 products Generating the Highest Revenue?
-- Simple Ranking

SELECT TOP 5
    p.product_name,
    SUM(s.sales_amount) AS total_revenue
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p
    ON p.product_key = s.product_key
GROUP BY p.product_name
ORDER BY total_revenue DESC;

-- Complex but Flexibly Ranking Using Window Functions

SELECT *
FROM (
    SELECT
        p.product_name,
        SUM(s.sales_amount) AS total_revenue,
        RANK() OVER (ORDER BY SUM(s.sales_amount) DESC) AS rank_products
    FROM gold.fact_sales s
    LEFT JOIN gold.dim_products p
        ON p.product_key = s.product_key
    GROUP BY p.product_name
) AS ranked_products
WHERE rank_products <= 5;

--  5 worst-performing products in terms of sales?

SELECT TOP 5
    p.product_name,
    SUM(s.sales_amount) AS total_revenue
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p
    ON p.product_key = s.product_key
GROUP BY p.product_name
ORDER BY total_revenue;

-- Top 10 customers who have generated the highest revenue

SELECT TOP 10
    c.customer_key,
    c.first_name,
    c.last_name,
    SUM(s.sales_amount) AS total_revenue
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c
    ON c.customer_key = s.customer_key
GROUP BY 
    c.customer_key,
    c.first_name,
    c.last_name
ORDER BY total_revenue DESC;

-- Bottom 3 customers with the fewest orders placed

SELECT TOP 3
    c.customer_key,
    c.first_name,
    c.last_name,
    COUNT(DISTINCT order_number) AS number_of_orders
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c
    ON c.customer_key = s.customer_key
GROUP BY 
    c.customer_key,
    c.first_name,
    c.last_name
ORDER BY number_of_orders ;

			-- ==============================================
			           -- Business queries --
			-- ==============================================
/*
===============================================================================
Change Over Time Analysis
===============================================================================
Purpose:
    - To track trends, growth, and changes in key metrics over time.
    - For time-series analysis and identifying seasonality.
    - To measure growth or decline over specific periods.

SQL Functions Used:
    - Date Functions: DATEPART(), DATETRUNC(), FORMAT()
    - Aggregate Functions: SUM(), COUNT(), AVG()
===============================================================================
*/

-- Analyse sales performance over time

SELECT
	DATETRUNC(MONTH,order_date) AS order_year,
	SUM(sales_amount) AS total_sales,
	AVG(sales_amount) AS avg_sales_year,
	COUNT(customer_key) AS total_customers,
	SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(MONTH,order_date)
ORDER BY order_year

/*
===============================================================================
Cumulative Analysis
===============================================================================
Purpose:
    - To calculate running totals or moving averages for key metrics.
    - To track performance over time cumulatively.
    - Useful for growth analysis or identifying long-term trends.

SQL Functions Used:
    - Window Functions: SUM() OVER(), AVG() OVER()
===============================================================================
*/

-- Calculate the total sales per month 
-- and the running total of sales over time 

SELECT
	order_date,
	total_sales,
	SUM(total_sales) OVER (PARTITION BY order_date ORDER BY order_date) AS running_total_sales,
	AVG(avg_price) OVER (PARTITION BY order_date ORDER BY order_date) AS moving_average_price
FROM
(
    SELECT 
        DATETRUNC(MONTH, order_date) AS order_date,
        SUM(sales_amount) AS total_sales,
        AVG(price) AS avg_price
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY DATETRUNC(MONTH, order_date)
) t

/*
===============================================================================
Performance Analysis (Year-over-Year, Month-over-Month)
===============================================================================
*/

/* Analyze the yearly performance of products by comparing their sales 
to both the average sales performance of the product and the previous year's sales */

WITH yearly_sales AS
(
SELECT
	YEAR(s.order_date) AS order_year,
	p.product_name,
	SUM(s.sales_amount) AS total_sales
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p
ON s.product_key=p.product_key
WHERE s.order_date IS NOT NULL
GROUP BY YEAR(s.order_date),p.product_name
)
SELECT
	order_year,
	product_name,
	total_sales,
	LAG(total_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS pre_year_sales,
	total_sales-LAG(total_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS sales_diff,
	CASE 
	WHEN total_sales-LAG(total_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'increased sales'
	WHEN total_sales-LAG(total_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'decreased sale'
	WHEN total_sales-LAG(total_sales) OVER (PARTITION BY product_name ORDER BY order_year) = 0 THEN 'Same as last year'
	END AS yearly_sales,
	AVG(total_sales) OVER (PARTITION BY product_name) AS avg_sales,
	total_sales-AVG(total_sales) OVER (PARTITION BY product_name) AS diff_in_avg,
	CASE 
	WHEN total_sales-AVG(total_sales) OVER (PARTITION BY product_name) > 0 THEN 'above avg'
	WHEN total_sales-AVG(total_sales) OVER (PARTITION BY product_name) < 0 THEN 'below avg'
	WHEN total_sales-AVG(total_sales) OVER (PARTITION BY product_name) = 0 THEN 'avg product'
	END AS product_condition
FROM yearly_sales
ORDER BY product_name,order_year

/*
===============================================================================
Part-to-Whole Analysis
===============================================================================
*/

-- categories contribution the most to overall sales?

WITH category_sales AS
(
SELECT
	category,
	SUM(sales_amount) AS total_sales
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p
ON s.product_key = p.product_key
GROUP BY category
)
SELECT
	category,
	total_sales,
	SUM(total_sales) OVER() AS gross_sales,
	CONCAT(ROUND((CAST((total_sales) AS FLOAT)/SUM(total_sales) OVER())*100,2),'%') AS contribution
FROM category_sales
ORDER BY total_sales DESC

/*
===============================================================================
Data Segmentation Analysis
===============================================================================
*/

/*Segment products into cost ranges and 
count how many products fall into each segment*/

WITH product_segments AS (
    SELECT
        product_key,
        product_name,
        cost,
        CASE 
            WHEN cost < 100 THEN 'Below 100'
            WHEN cost BETWEEN 100 AND 500 THEN '100-500'
            WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
            ELSE 'Above 1000'
        END AS cost_range
    FROM gold.dim_products
)
SELECT 
    cost_range,
    COUNT(product_key) AS total_products
FROM product_segments
GROUP BY cost_range
ORDER BY total_products DESC;

/*Group customers into three segments based on their spending behavior:
	- VIP: Customers with at least 12 months of history and spending more than €5,000.
	- Regular: Customers with at least 12 months of history but spending €5,000 or less.
	- New: Customers with a lifespan less than 12 months.
And find the total number of customers by each group
*/

WITH customer_spending AS (
    SELECT
        c.customer_key,
        SUM(s.sales_amount) AS total_spent,
        MIN(order_date) AS first_order,
        MAX(order_date) AS last_order,
        DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan
    FROM gold.fact_sales s
    LEFT JOIN gold.dim_customers c
        ON s.customer_key = c.customer_key
    GROUP BY c.customer_key
)
SELECT 
    customer_segment,
    COUNT(customer_key) AS total_customers
FROM (
    SELECT 
        customer_key,
        CASE 
            WHEN lifespan >= 12 AND total_spent > 5000 THEN 'VIP customer'
            WHEN lifespan >= 12 AND total_spent <= 5000 THEN 'Regular customer'
            ELSE 'New customer'
        END AS customer_segment
    FROM customer_spending
) AS segmented_customers
GROUP BY customer_segment
ORDER BY total_customers DESC;

/*
===============================================================================
                           Customer Report
===============================================================================
*/

/*Highlights:
    1. Gathers essential fields such as names, ages, and transaction details.
	2. Segments customers into categories (VIP, Regular, New) and age groups.
    3. Aggregates customer-level metrics:
	   - total orders
	   - total sales
	   - total quantity purchased
	   - total products
	   - lifespan (in months)
    4. Calculates valuable KPIs:
	    - recency (months since last order)
		- average order value
		- average monthly spend
*/

IF OBJECT_ID('gold.customer_report', 'V') IS NOT NULL
    DROP VIEW gold.customer_report;
GO

CREATE VIEW gold.customer_report AS

WITH customer_details AS(
SELECT
s.order_number,
s.product_key,
s.order_date,
s.sales_amount,
s.quantity,
c.customer_key,
c.customer_number,
CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
DATEDIFF(year, c.birthdate, GETDATE()) age
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c
ON c.customer_key = s.customer_key
WHERE s.order_date IS NOT NULL),

customer_aggregation AS (
SELECT 
	customer_key,
	customer_number,
	customer_name,
	age,
	COUNT(DISTINCT order_number) AS total_orders,
	SUM(sales_amount) AS total_sales,
	SUM(quantity) AS total_quantity,
	COUNT(DISTINCT product_key) AS total_products,
	MAX(order_date) AS last_order_date,
	DATEDIFF(month, MIN(order_date), MAX(order_date)) AS gap_in_orders
FROM customer_details
GROUP BY 
	customer_key,
	customer_number,
	customer_name,
	age
)
SELECT
customer_key,
customer_number,
customer_name,
age,
CASE 
	 WHEN age < 20 THEN 'Under 20'
	 WHEN age between 20 and 29 THEN '20-29'
	 WHEN age between 30 and 39 THEN '30-39'
	 WHEN age between 40 and 49 THEN '40-49'
	 ELSE '50 and above'
END AS age_group,
CASE 
    WHEN gap_in_orders >= 12 AND total_sales > 5000 THEN 'VIP customer'
    WHEN gap_in_orders >= 12 AND total_sales <= 5000 THEN 'Regular customer'
    ELSE 'New customer'
END AS customer_segment,
last_order_date,
DATEDIFF(month, last_order_date, GETDATE()) AS recency,
total_orders,
total_sales,
total_quantity,
total_products
lifespan,
CASE WHEN total_sales = 0 THEN 0
	 ELSE total_sales / total_orders
END AS avg_order_value,
CASE WHEN gap_in_orders = 0 THEN total_sales
     ELSE total_sales / gap_in_orders
END AS avg_monthly_spend
FROM customer_aggregation

/*
===============================================================================
                       Product Report
===============================================================================
*/

IF OBJECT_ID('gold.product_report', 'V') IS NOT NULL
    DROP VIEW gold.product_report;
GO

CREATE VIEW gold.product_report AS

WITH product_details AS (
    SELECT
	    s.order_number,
        s.order_date,
		s.customer_key,
        s.sales_amount,
        s.quantity,
        p.product_key,
        p.product_name,
        p.category,
        p.subcategory,
        p.cost
    FROM gold.fact_sales s
    LEFT JOIN gold.dim_products p
        ON s.product_key = p.product_key
    WHERE s.order_date IS NOT NULL  
),
product_aggregations AS (
SELECT
    product_key,
    product_name,
    category,
    subcategory,
    cost,
    DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS order_gap_month,
    MAX(order_date) AS last_sale_date,
    COUNT(DISTINCT order_number) AS total_orders,
	COUNT(DISTINCT customer_key) AS total_customers,
    SUM(sales_amount) AS total_sales,
    SUM(quantity) AS total_quantity,
	ROUND(AVG(CAST(sales_amount AS FLOAT) / NULLIF(quantity, 0)),1) AS avg_selling_price
FROM product_details

GROUP BY
    product_key,
    product_name,
    category,
    subcategory,
    cost
)
SELECT 
	product_key,
	product_name,
	category,
	subcategory,
	cost,
	last_sale_date,
	DATEDIFF(MONTH, last_sale_date, GETDATE()) AS recency_in_months,
	CASE
		WHEN total_sales > 50000 THEN 'best selling'
		WHEN total_sales >= 10000 THEN 'Mid-Range'
		ELSE 'Low-Performer'
	END AS product_segment,
	order_gap_month,
	total_orders,
	total_sales,
	total_quantity,
	total_customers,
	avg_selling_price,
	CASE 
		WHEN total_orders = 0 THEN 0
		ELSE total_sales / total_orders
	END AS avg_order_revenue,
	CASE
		WHEN order_gap_month = 0 THEN total_sales
		ELSE total_sales / order_gap_month
	END AS avg_monthly_revenue
FROM product_aggregations 

SELECT * FROM gold.product_report
