# Databricks notebook source
# MAGIC %md
# MAGIC #### Create enriched customers table
# MAGIC - Handle null values
# MAGIC - Correct data types
# MAGIC - Data quality checks and correct any schema issues

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Configuration Parameters

# COMMAND ----------

silver_location = "dbfs:/FileStore/assignment/silver/enriched_orders"

# Table name
bronze_table_name = "bronze.orders"
table_name = "silver.enriched_orders"
product_table_name = "silver.products_master"
customer_table_name = "silver.customers_master"

# COMMAND ----------

dbutils.widgets.text("bronze_processing_date", "2024-03-18")
bronze_processing_date = dbutils.widgets.get('bronze_processing_date')

# COMMAND ----------

# Retrieve the currently added data

from pyspark.sql.functions import to_date, col

bronze_orders_df = (spark.read
             .table(bronze_table_name)
             .filter(to_date(col("_processing_date")) == f'{bronze_processing_date}')
            )

# COMMAND ----------

# MAGIC %md 
# MAGIC 1. Do data quality checks

# COMMAND ----------

data_dq_check = bronze_orders_df.filter(col("row_id").isNull())
data_dq_clean = bronze_orders_df.subtract(data_dq_check)
print("Rows with null order ids: ", data_dq_check.count())

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Data Deduplication

# COMMAND ----------

from pyspark.sql.functions import to_date, row_number
from pyspark.sql import Window

#Assumption [order_id] as a primary key
data_window_spec = Window.partitionBy("row_id").orderBy(to_date(col("_processing_date")).desc())
latest_orders = (data_dq_clean
                   .withColumn("row_number", row_number().over(data_window_spec))
                   .filter("row_number = 1").drop("row_number"))

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Correct Data Structure issue

# COMMAND ----------

from pyspark.sql.functions import to_date, col
from pyspark.sql.types import DoubleType

orders_schema_df = (latest_orders
                    .withColumn("order_date", to_date(col("order_date"), "d/M/yyyy"))
                    .withColumn("ship_date", to_date(col("ship_date"), "d/M/yyyy"))
                    .withColumn("profit", col("profit").cast(DoubleType()))
                  )

# COMMAND ----------

# MAGIC %md 
# MAGIC Enrich Orders table

# COMMAND ----------

from pyspark.sql.functions import round, col
profit_rounded = orders_schema_df.withColumn("profit", round(col("profit"), 2))

# COMMAND ----------

from pyspark.sql.functions import when, date_trunc, current_timestamp
from datetime import date

processing_date = date_trunc('second', current_timestamp())
processing_date_ = date.today()

# COMMAND ----------

from pyspark.sql.functions import to_date, lit
customers_df =  (spark.read
             .table(customer_table_name)
             .filter(to_date(col("_processing_date")) == f'{processing_date_}')
            )

# COMMAND ----------

products_df = (spark.read
             .table(product_table_name)
             .filter(to_date(col("_processing_date")) == f'{processing_date_}')
            )

# COMMAND ----------

joined_cust_df = (profit_rounded.join(customers_df, customers_df["customer_id"] == profit_rounded["customer_id"], "left")
             .select(profit_rounded["*"], customers_df["customer_name"], customers_df["country"])
)
joined_df = (joined_cust_df.join(products_df, joined_cust_df["product_id"] == products_df["product_id"], "left")
             .select(joined_cust_df["*"], products_df["category"], products_df["sub_category"])
             .withColumn("_processing_date", processing_date)
          )

# COMMAND ----------

# MAGIC %md
# MAGIC Merge into Silver Table

# COMMAND ----------

from delta.tables import DeltaTable

# check if the silverLocation contain the delta table
if(DeltaTable.isDeltaTable(spark, silver_location)): 

    DeltaTable.forPath(spark, silver_location).alias("target").merge(
        source = joined_df.alias("src"),
        condition = "src.order_id = target.order_id"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll().execute()
else:
    # If no, save the file to silverLocation
    joined_df.write.mode("overwrite").format("delta").save(silver_location)

# COMMAND ----------

# create the database and table, if required

spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING delta LOCATION '{silver_location}'")