-- Databricks notebook source
-- MAGIC %md
-- MAGIC Profit by year

-- COMMAND ----------

select year, profit_by_year
from
gold.profit_by_year
order by year;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Profit by year + product category

-- COMMAND ----------

select year(order_date) as year,
nvl(category, 'NA') as product_category,
round(sum(profit), 2) as profit
from silver.enriched_orders
group by year(order_date), category
order by year, product_category;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC profit by customer

-- COMMAND ----------

select customer_id, customer_name,
profit_by_customer
from gold.profit_by_customer
order by customer_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC profit by customer + year

-- COMMAND ----------

select year(order_date) as year,
customer_id,
min(customer_name) as customer_name,
round(sum(profit), 2) as profit
from silver.enriched_orders
group by year(order_date), customer_id
order by year, customer_id;