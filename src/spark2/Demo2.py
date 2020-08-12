# coding=utf-8

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, DecimalType
import pyspark.sql.functions as func
import decimal

# 获取spark的上下文
sc = SparkContext.getOrCreate()
sc.setLogLevel('WARN')
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# 读取本地或HDFS上的文件【.load('hdfs://S0:8020/input/movies.csv')】
df = spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('hdfs://S0:8020/input/movies.csv')
print(df.dtypes)


def get_month(x):
    motnth = x.split('.')[0] + '-' + x.split('.')[1]
    print(motnth)
    return motnth


# func_month = udf(lambda x: x.split('.')[0] + '-' + x.split('.')[1], StringType())
func_month = udf(lambda x: get_month(x), StringType())
df = df.withColumn('month', func_month(col('begin_date')))

func_box_office = udf(lambda x: decimal.Decimal(x[0:-1]), DecimalType(5, 2))
df = df.withColumn('box_office_number', func_box_office(col('box_office')))

# 选择两个列
df = df.select(["month", "box_office_number"])

df.show()
df.printSchema()

# 创建临时数据视图info
df.createOrReplaceTempView("info")

# 各月份的票房总和
df = df.groupBy("month").agg(func.sum("box_office_number")).sort(df["month"].asc())
# 列重命名
df = df.withColumnRenamed('sum(box_office_number)' , 'sum');
df.show()

df_result1 = spark.sql("select month,sum(box_office_number) sum,count(month) count from info group by month order by month desc limit 10")
df_result1.show()

# 月份的最大票房
df_result2 = spark.sql("select max(box_office_number) max from info")
df_result2.show()

# 月份的最小票房
df_result3 = spark.sql("select min(box_office_number) min from info")
df_result3.show()

# 月份的票房总和
df_result4 = spark.sql("select sum(box_office_number) sum from info")
df_result4.show()

# 将结果保存为JSON
df_result4.write.json("../data/result4.json")
