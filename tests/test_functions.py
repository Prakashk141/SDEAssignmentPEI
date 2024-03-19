# Databricks notebook source
# MAGIC %run ../silver/functions

# COMMAND ----------

from pyspark.sql.types import Row, StructType, StructField, StringType
from pyspark.sql.functions import col

# COMMAND ----------

def test_handle_product_name():
  schema = StructType([
        StructField("product_name", StringType() ,False)
    ])
  expected_schema = StructType([
      StructField("product_name", StringType() ,False)
  ])
  data = [
      Row("Test Product"),
      Row('''"Test Product 2"" x 2"""'''),
  ]
  expected_data = [
      Row("Test Product"), 
      Row('''Test Product 2" x 2"''')
  ]
  input_df = spark.createDataFrame(data, schema)
  expected_df = spark.createDataFrame(expected_data, expected_schema)
  
  actual_df = handle_product_name(input_df, "product_name")
  
  #Assert
  assert actual_df.schema == expected_df.schema
  assert actual_df.collect() == expected_df.collect()
  print("Test Passed !")

test_handle_product_name()

# COMMAND ----------

def test_normalize_phone_no():
  schema = StructType([
        StructField("id", StringType() ,False),
        StructField("phone", StringType() ,False)
    ])
  expected_schema = StructType([
      StructField("id", StringType(),False),
      StructField("phone", StringType(),True)
  ])
  data = [
      Row("1", "421.580.0902"), 
      Row("2", "542-415-0246"), 
      Row("3", "7185624866"), 
      Row("4", "(563)647-4830"),
      Row("5", "775-548-2891x2689")
  ]
  expected_data = [ 
      Row("1", "421-580-0902"), 
      Row("2", "542-415-0246"), 
      Row("3", "718-562-4866"), 
      Row("4", "563-647-4830"),
      Row("5", "775-548-2891")
  ]
  input_df = spark.createDataFrame(data, schema)
  expected_df = spark.createDataFrame(expected_data, expected_schema)
  
  actual_df = normalize_phone_no(input_df, "phone")
  
  #Assert
  assert actual_df.schema == expected_df.schema
  assert actual_df.collect() == expected_df.collect()
  print("Test Passed !")

test_normalize_phone_no()

# COMMAND ----------

def test_normalize_customer_name():
  schema = StructType([
        StructField("id", StringType() ,False),
        StructField("name", StringType() ,True)
    ])
  expected_schema = StructType([
      StructField("id", StringType(),False),
      StructField("name", StringType(),True)
  ])
  data = [
      Row("1", "Ad.  ..am Hart"), 
      Row("2", "Tam&^*ara Willing___)ham"), 
      Row("3", "Russell D'Ascenzo"), 
      Row("4", "B         ecky Martin"),
      Row("5", "Shirl)(*&ey Schmidt"),
      Row("6", None)
  ]
  expected_data = [ 
      Row("1", "Adam Hart"), 
      Row("2", "Tamara Willingham"), 
      Row("3", "Russell D'Ascenzo"), 
      Row("4", "Becky Martin"),
      Row("5", "Shirley Schmidt"),
      Row("6", "NA")
  ]
  input_df = spark.createDataFrame(data, schema)
  expected_df = spark.createDataFrame(expected_data, expected_schema)
  
  actual_df = normalize_customer_name(input_df, "name")
  
  #Assert
  assert actual_df.schema == expected_df.schema
  assert actual_df.collect() == expected_df.collect()
  print("Test Passed !")

test_normalize_customer_name()