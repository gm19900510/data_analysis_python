# coding=utf-8

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, DecimalType
import pyspark.sql.functions as func
import decimal

# 创建SparkContext
sc = SparkContext('local')
sc.setLogLevel('WARN')

# 创建SQLContext
sqlContext = SQLContext(sc)

# 读取本地或HDFS上的文件【.load('../data/movies.csv')】、【.load('hdfs://192.168.3.9:8020/input/movies.csv')】
df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('../data/movies.csv')

# 打印列的数据类型
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

df_result1 = sqlContext.sql("select month,sum(box_office_number) sum,count(month) count from info group by month order by month desc limit 10")
df_result1.show()

# 月份的最大票房
df_result2 = sqlContext.sql("select max(box_office_number) max from info")
df_result2.show()

# 月份的最小票房
df_result3 = sqlContext.sql("select min(box_office_number) min from info")
df_result3.show()

# 月份的票房总和
df_result4 = sqlContext.sql("select sum(box_office_number) sum from info")
df_result4.show()

# 将结果保存为JSON
df_result4.write.json("../data/result4.json")
