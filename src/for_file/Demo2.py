# -*- coding: utf-8 -*-
from pyspark import SparkContext
from pyspark.sql import SQLContext
import json
import pandas as pd

'''
当需要把Spark DataFrame转换成Pandas DataFrame时，可以调用toPandas()；
当需要从Pandas DataFrame创建Spark DataFrame时，可以采用createDataFrame(pandas_df)。
但是，需要注意的是，在调用这些操作之前，
需要首先把Spark的参数spark.sql.execution.arrow.enabled设置为true，
因为这个参数在默认情况下是false
'''

# 所需字段和新老字段映射关系
columns_json_str = '{"name":"影片名称","box_office":"票房"}'
columns_dict = json.loads(columns_json_str)

# 创建SparkContext
sc = SparkContext('local')

# 创建SQLContext
sqlContext = SQLContext(sc)

# 读取本地或HDFS上的文件【.load('hdfs://192.168.3.9:8020/input/movies.csv')】
df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('../data/movies.csv')

# 打印列数据类型
print(df.dtypes)

# 将spark.dataFrame转为pandas.DataFrame，在此处选取指定的列
df = pd.DataFrame(df.toPandas(), columns=columns_dict.keys())
print(df)

data_values = df.values.tolist()
data_coulumns = list(df.columns)

# 将pandas.DataFrame转为spark.dataFrame，需要转数据和列名
df = sqlContext.createDataFrame(data_values, data_coulumns)

# 字段重命名
# df = df.withColumnRenamed('name', '影片名称') 
for key in columns_dict.keys() :
    df = df.withColumnRenamed(key , columns_dict[key]);

print(df.collect())
print(df.printSchema())

# 将重命名之后的数据写入到文件
filepath = 'new_movies.csv'
df.write.format("csv").options(header='true', inferschema='true').save('hdfs://192.168.3.9:8020/input/' + filepath)
