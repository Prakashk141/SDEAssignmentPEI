# Databricks notebook source
# MAGIC %md
# MAGIC ##### function to handle anomalies in Customer's Name

# COMMAND ----------

def normalize_customer_name(df, col_name):
  from pyspark.sql.functions import when, regexp_replace, trim, col

  pattern = r"[^a-zA-Z\s']"
  return (df.withColumn(col_name,
                            regexp_replace(
                              regexp_replace(
                              trim(col(col_name)), pattern, ""
                              )
                            , "[\s']{2,}|([a-z])[\s']([a-z])", "$1$2"
                            )
                          )
                          .withColumn(col_name, when(col(col_name).isNull(), "NA").otherwise(col(col_name)))
          )
  

# COMMAND ----------

# MAGIC %md
# MAGIC ##### function to handle anomalies in Product's Name

# COMMAND ----------

def handle_product_name(df, col_name):
  from pyspark.sql.functions import regexp_replace, col
  
  return df.withColumn(col_name, regexp_replace(regexp_replace(col(col_name), '^"|"$', ''), '""', '"'))

# COMMAND ----------

# MAGIC %md
# MAGIC Normalize customer phone numbers

# COMMAND ----------

def normalize_phone_no(df, col_name):
  from pyspark.sql.functions import when, length, regexp_replace, regexp_extract, concat_ws, col
  
  corrected_phone_no = (df
                        .withColumn(col_name
                                    ,when(col(col_name) == '#ERROR!', None)
                                    .when(length(col(col_name)) < 10, None)
                                    .otherwise(regexp_replace(
                                      regexp_replace(
                                      regexp_replace(col(col_name), "x\d+", "")
                                      ,"001[-.]","")
                                      ,'\D', "")))
                        .withColumn(col_name,
                                  when(col(col_name).isNotNull(), concat_ws("-", 
                                            regexp_extract(col(col_name), r'(\d{3})', 1),
                                            regexp_extract(col(col_name), r'(\d{3})(\d{3})', 2),
                                            regexp_extract(col(col_name), r'(\d{4})$', 1))))
                        )
  return corrected_phone_no