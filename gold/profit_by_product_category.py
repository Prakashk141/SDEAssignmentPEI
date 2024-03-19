# Databricks notebook source
# MAGIC %md
# MAGIC ##### Configuration Parameters

# COMMAND ----------

gold_location = "dbfs:/FileStore/assignment/gold/profit_by_product_category"

# Table name
silver_table_name = "silver.enriched_orders"
table_name = "gold.profit_by_product_category"

# COMMAND ----------

dbutils.widgets.text("silver_processing_date", "2024-03-19")
bronze_processing_date = dbutils.widgets.get('silver_processing_date')

# COMMAND ----------

# Retrieve the currently added data

enriched_order_df = (spark.read
             .table(silver_table_name)
            )

# COMMAND ----------

from pyspark.sql.functions import sum, round, col

grouped_df = (enriched_order_df
            .groupby(col("category"))
            .agg(round(sum(col("profit")), 2).alias("profit_by_category"))
          )

# COMMAND ----------

# MAGIC %md 
# MAGIC Handle nulls

# COMMAND ----------

final_df = grouped_df.fillna({"category": "NA"})

# COMMAND ----------

# MAGIC %md
# MAGIC Create gold layer table

# COMMAND ----------

from delta.tables import DeltaTable

# check if the silverLocation contain the delta table
if(DeltaTable.isDeltaTable(spark, gold_location)): 

    DeltaTable.forPath(spark, gold_location).alias("target").merge(
        source = final_df.alias("src"),
        condition = "src.category = target.category"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll().execute()
else:
    # If no, save the file to silverLocation
    final_df.write.mode("overwrite").format("delta").save(gold_location)

# COMMAND ----------

# create the database and table, if required

spark.sql("CREATE DATABASE IF NOT EXISTS gold")
spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING delta LOCATION '{gold_location}'")