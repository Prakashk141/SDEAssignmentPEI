# Databricks notebook source
# MAGIC %md
# MAGIC #### Define variables required to run the notebook

# COMMAND ----------

landing_file_location = "dbfs:/FileStore/assignment/landing/product"
bronze_location = "dbfs:/FileStore/assignment/raw/products"

# Bronze Products Table name
table_name = "bronze.products"

# COMMAND ----------

products_df = (spark.read.format("csv")
               .option("header", True)
               .option("path", f"{landing_file_location}/Product.csv")
               .load()
)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, date_trunc

# Add processing_date column to the raw customer table
processing_date = date_trunc('second', current_timestamp())

products_df = (products_df
                .withColumn("_processing_date", processing_date)
              )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Standardize all the column names to lowercase and remove any special characters

# COMMAND ----------

import re
products_final_df = products_df.toDF(*[re.sub('[ ,-;{}()\n\t="]', '_', c.lower()) for c in products_df.columns])

# COMMAND ----------

from delta.tables import DeltaTable

if(DeltaTable.isDeltaTable(spark, bronze_location)): 
    # If Delta table exists, append the data 
    products_final_df.write.mode("append").format("delta").save(bronze_location)

else:
    # If no, save the file to location
    products_final_df.write.mode("overwrite").format("delta").save(bronze_location)

# COMMAND ----------

# create the database and table, if required
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING delta LOCATION '{bronze_location}'")