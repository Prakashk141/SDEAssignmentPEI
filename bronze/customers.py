# Databricks notebook source
# MAGIC %md
# MAGIC #### Define variables required to run the notebook

# COMMAND ----------

landing_file_location = "dbfs:/FileStore/assignment/landing/customer"
bronze_location = "dbfs:/FileStore/assignment/raw/customers"

# Bronze Customer Table name
table_name = "bronze.customers"

# COMMAND ----------

# Read the customer data from the landing zone
# Set Spark configuration to use package for reading excel files
spark.conf.set("spark.jar.packages", "com.crealytics:spark-excel_2.12:3.5.0_0.20.3")
customers_df = (spark.read.format("com.crealytics.spark.excel")
                  .option("header", True)
                  .option("treatEmptyValuesAsNulls", "true")
                  .option("path", f"{landing_file_location}/Customer.xlsx")
                  .load()
)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, date_trunc

# Add processing_date column to the raw customer table
processing_date = date_trunc('second', current_timestamp())

customers_df = (customers_df
                .withColumn("_processing_date", processing_date)
              )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Standardize all the column names to lowercase and remove any special characters

# COMMAND ----------

import re
customers_final_df = customers_df.toDF(*[re.sub('[ ,-;{}()\n\t="]', '_', c.lower()) for c in customers_df.columns])

# COMMAND ----------

from delta.tables import DeltaTable

if(DeltaTable.isDeltaTable(spark, bronze_location)): 
    # If Delta table exists, append the data 
    customers_final_df.write.mode("append").format("delta").save(bronze_location)

else:
    # If no, save the file to location
    customers_final_df.write.mode("overwrite").format("delta").save(bronze_location)


# COMMAND ----------

# create the database and table, if required
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING delta LOCATION '{bronze_location}'")