# coding=utf-8

from datetime import datetime
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType
from pyspark import SparkContext
from pyspark.sql import SQLContext

# 创建SparkContext
sc = SparkContext('local')

# 创建SQLContext
sqlContext = SQLContext(sc)

# 创建DataFrame
df = sqlContext.createDataFrame([("11/25/1991", "11/24/1991", "11/30/1991"),

                            ("11/25/1391", "11/24/1992", "11/30/1992")], schema=['first', 'second', 'third'])

# 调用withColumn基于原first列进行数据类型转换生成新列test
func = udf (lambda x: datetime.strptime(x, '%m/%d/%Y'), DateType())
df = df.withColumn('test', func(col('first')))

# 数据打印
df.show()

# 打印元数据信息
df.printSchema()
