# coding=utf-8

from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import concat
import os
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/CDH-5.16.2-1.cdh5.16.2.p0.8/lib/spark"

# 获取spark的上下文
sc = SparkContext.getOrCreate()
hive_context = HiveContext(sc)

# 生成查询的SQL语句，这个跟hive的查询语句一样，所以也可以加where等条件语句
hive_database = "db_hive_test"
hive_table = "hive_movies"
hive_read = "select * from  {}.{}".format(hive_database, hive_table)
 
# 通过SQL语句在hive中查询的数据直接是dataframe的形式
read_df = hive_context.sql(hive_read)

# 过滤所有空值
read_df = read_df.na.drop()

read_df.show()
read_df.printSchema()

# read_df = read_df.select(concat(*read_df.columns).alias('data'))
# read_df.write.format("text").option("header", "false").mode("append").save("hdfs://s0:8020/input/myjson")

# read_df.write.format("com.databricks.spark.csv").save("hdfs://s0:8020/input/mycsv.csv")

read_df.write.format("json").save("hdfs://s0:8020/input/myjson.json")
