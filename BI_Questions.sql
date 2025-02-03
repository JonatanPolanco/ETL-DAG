-- total revenue per product
-- fact_table table cluster by product_id to avoid full scann of the table (easy access by product blocks)
SELECT 
prod.category AS product_category,
SUM(fact.amount) AS total_sales_revenue
FROM DBT_DB.DBT_SCHEMA.FACT_TABLE fact
JOIN DBT_DB.DBT_SCHEMA.PRODUCTS prod 
ON fact.product_id = prod.product_id
GROUP BY category
ORDER BY total_sales_revenue DESC

--Top 5 customers by total purchase amount.
-- Partition by customer_id for gruoping custommers in fact_table can be faster

SELECT 
fact.customer_id,
name AS customer_name,
SUM(fact.amount) AS purchase_amount,
RANK() OVER (ORDER BY SUM(fact.amount) DESC) AS top_rank
FROM DBT_DB.DBT_SCHEMA.FACT_TABLE fact
JOIN DBT_DB.DBT_SCHEMA.customers cmer ON fact.customer_id = cmer.customer_id
GROUP BY fact.customer_id,name
ORDER BY purchase_amount DESC
LIMIT 5


---Monthly sales trends for the last 12 months.
-- Partition by TRANSACTION_DATE will optimize calculations and reduce query costs

SELECT 
  DATE_TRUNC('month', transaction_date) AS sales_month,
  SUM(amount) AS total_sales
FROM DBT_DB.DBT_SCHEMA.FACT_TABLE
WHERE 
  transaction_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '12 months')
GROUP BY sales_month
ORDER BY sales_month;





