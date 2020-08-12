# coding=utf-8

from datetime import datetime
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType
from pyspark import SparkContext
from pyspark.sql import SparkSession

# 获取spark的上下文
sc = SparkContext.getOrCreate()
spark = SparkSession.builder.getOrCreate()

# 创建DataFrame
df = spark.createDataFrame([("11/25/1991", "11/24/1991", "11/30/1991"),

                            ("11/25/1391", "11/24/1992", "11/30/1992")], schema=['first', 'second', 'third'])

# 调用withColumn基于原first列进行数据类型转换生成新列test
func = udf (lambda x: datetime.strptime(x, '%m/%d/%Y'), DateType())
df = df.withColumn('test', func(col('first')))

df.show()
df.printSchema()
