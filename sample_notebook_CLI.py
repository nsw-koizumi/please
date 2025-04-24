# Databricks notebook source

from pyspark.sql import SparkSession
from pyspark.sql.types import *

# SparkSessionの作成
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# スキーマの定義
schema = StructType([
  StructField('CustomerID', IntegerType(), False),
  StructField('FirstName', StringType(), False),
  StructField('LastName', StringType(), False)
])

# COMMAND ----------

# サンプルデータの作成
data = [
  [1000, '山田', '恭子'],
  [1001, '鈴木', '花子'],
  [1002, '今野', 'ドラゴン'],
  [1002, '佐藤', 'サティアン'],
  [1002, '新井', 'wakaba'],
  [1002, '新井', 'wakaba2'],
  [1002, '新井', 'wakaba3']
]

# DataFrameの作成と表示
customers = spark.createDataFrame(data, schema)
customers.show()
