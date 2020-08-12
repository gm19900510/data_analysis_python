# coding=utf-8

from pyspark.sql import HiveContext
from pyspark import SparkContext

# 获取spark的上下文
sc = SparkContext.getOrCreate()
hive_context = HiveContext(sc)

data = [
    (1, "zhangsan", "1"),
    (2, "lisi", "1"),
    (2, "wangwu", "1"),
]
df = hive_context.createDataFrame(data, ['id', "name", 'age'])
  
# 创建临时表
df.registerTempTable('temp_hive')

df = hive_context.sql("select * from temp_hive")
df.show()

# 在Hive中创建表并写入数据
hive_context.sql("create table db_hive_test.spark_hive as select * from temp_hive")

df2 = hive_context.sql("select * from db_hive_test.spark_hive")
df2.show()
