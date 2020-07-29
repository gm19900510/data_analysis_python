# -*- coding: utf-8 -*-
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
import json

# 所需字段和新老字段映射关系
columns_json_str = '{"name":"NEW_NAME","src":"NEW_SRC"}'
columns_dict = json.loads(columns_json_str)

# 获取spark的上下文
sc = SparkContext('local', 'spark_file_conversion')
sc.setLogLevel('WARN')
spark = SparkSession.builder.getOrCreate()

# 构建StructField和StructType用于读取指定字段
fields = []
for key in columns_dict.keys() :
    print(key , columns_dict[key])
    fields.append(StructField(key, StringType(), True))

schema = StructType(fields)

# 读取本地或HDFS上的文件【.load('hdfs://192.168.3.9:8020/input/movies.csv')】
df = spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').schema(schema).load('hdfs://192.168.3.9:8020/input/movies.csv')

# 字段重命名
# df.rename(columns={'name':'NEW_NAME', 'src':'NEW_SRC'}, inplace=True) 
for key in columns_dict.keys() :
    df = df.withColumnRenamed(key , columns_dict[key]);
print(df)
print(df.printSchema())

# 将重命名之后的数据写入到文件
filepath = 'new_movies1.csv'
df.write.format("csv").options(header='true', inferschema='true').save('hdfs://192.168.3.9:8020/input/' + filepath)
