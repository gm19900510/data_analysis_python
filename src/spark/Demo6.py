# coding=utf-8

from pyspark import SparkContext
from dependency.mydata import data  # 自己写的模块

# 获取spark的上下文
sc = SparkContext()
sc.setLogLevel('WARN')

out = sc.parallelize(data)
print(out.collect())

# out是RDD格式需调用.toDF()转为spark.dataFrame格式
df = out.toDF()
df.show()

out.saveAsTextFile("hdfs://s0:8020/input/text")
