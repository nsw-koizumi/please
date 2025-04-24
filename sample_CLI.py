from pyspark.sql import SparkSession
from pyspark.sql.types import *

# SparkSessionの作成
spark = SparkSession.builder.getOrCreate()

# スキーマの定義
schema = StructType([
  StructField('CustomerID', IntegerType(), False),
  StructField('FirstName', StringType(), False),
  StructField('LastName', StringType(), False)
])

# サンプルデータの作成
data = [
  [1000, '山田', '太郎'],
  [1001, '鈴木', '花子'],
  [1002, '佐藤', '次郎']
]

# DataFrameの作成と表示
customers = spark.createDataFrame(data, schema)
customers.show()
