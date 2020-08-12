# data_analysis_python

利用python操作txt、csv、excel文件，及简单数据分析，适用于`Spark1.6.0`、`Spark2.3.0`

## 读取操作本地或HDFS上的csv、xls文件
 
- for_file/Demo.py

  包含：读取本地`csv`文件、选取指定列、更改列名、数据打印、保存`csv`文件

- for_file/Demo2.py

  包含：利用`pyspark`读取本地或`HDFS`上的`csv`文件、选取指定列、更改列名、创建临时视图、各类`SQL`统计、数据展示、保存`csv`文件
  
- for_file/Demo3.py
  
  包含：读取本地`xls`文件、选取指定列、更改列名、数据打印、保存`xls`文件

## pyspark数据分析，适用于spark1.6版本

- spark/Demo.py

  包含：创建`spark.DataFrame`、调用`udf`对指定列数据进行格式转换生成新列

- spark/Demo2.py

  包含：利用`pyspark`读取本地或`HDFS`上的`csv`文件、调用`udf`对指定列数据进行格式转换生成新列、选取指定列、各类`SQL`统计、数据打印、保存`JSON`文件
  
- spark/Demo3.py
  
  包含：利用`pd`读取本地的`xls`文件、`pandas.DataFrame`转`spark.dataFrame`、调用`udf`对指定列数据进行格式转换生成新列、选取指定列、各类`SQL`统计、数据打印、保存`JSON`文件
  
- spark/Demo4.py
  
  包含：创建`spark.DataFrame`、创建临时表、根据临时表在`Hive`中建表并导入数据、查询`Hive`中新表
  
- spark/Demo5.py
  
  包含：读取Hive上的表、过滤所有空值、并以JSON格式保存回HDFS 
  
  `spark-submit --master yarn --deploy-mode cluster --py-files /root/Demo5.py `

- spark/Demo6.py
  
  包含：引入自定义模块、`array`转`RDD`、`RDD`转`spark.DataFrame`、并以`text`格式保存回`HDFS`
  
  `spark-submit --master yarn --deploy-mode cluster --py-files /root/dep.zip /root/Demo6.py `

## pyspark2数据分析，适用于spark2.3版本 

- spark2/Demo.py

  包含：创建`spark.DataFrame`、调用`udf`对指定列数据进行格式转换生成新列

- spark2/Demo2.py

  包含：利用`pyspark`读取本地或`HDFS`上的`csv`文件、调用`udf`对指定列数据进行格式转换生成新列、选取指定列、各类`SQL`统计、数据打印、保存`JSON`文件
  
- spark2/Demo3.py
  
  包含：利用`pd`读取本地的`xls`文件、`pandas.DataFrame`转`spark.dataFrame`、调用`udf`对指定列数据进行格式转换生成新列、选取指定列、各类`SQL`统计、数据打印、保存`JSON`文件
  
- spark2/Demo4.py
  
  包含：创建`spark.DataFrame`、创建临时表、根据临时表在`Hive`中建表并导入数据、查询`Hive`中新表
  
- spark2/Demo5.py
  
  包含：读取Hive上的表、过滤所有空值、并以JSON格式保存回HDFS 
  
  `spark2-submit --master yarn --deploy-mode cluster --py-files /root/Demo5.py `

- spark2/Demo6.py
  
  包含：引入自定义模块、`array`转`RDD`、`RDD`转`spark.DataFrame`、并以`text`格式保存回`HDFS`
  
  `spark2-submit --master yarn --deploy-mode cluster --py-files /root/dep.zip /root/Demo6.py `

## 两个文件夹文件比对

- tools/dir_compare.py
  
## 安装pyspark 1.6.0

  pip install pyspark==1.6.0 -i http://pypi.douban.com/simple --trusted-host pypi.douban.com
  
  pip install py4j -i http://pypi.douban.com/simple --trusted-host pypi.douban.com
  
  pip install pyarrow -i http://pypi.douban.com/simple --trusted-host pypi.douban.com
  
  或
  
  cp -r /opt/cloudera/parcels/CDH-5.16.2-1.cdh5.16.2.p0.8/lib/spark/python/pyspark /usr/lib64/python2.7/site-packages
  
  yum -y install epel-release
  
  yum install -y python-pip
  
  pip install py4j==0.10.7 -i http://pypi.douban.com/simple --trusted-host pypi.douban.com

## 安装pyspark 2.3.0

  pip install pyspark==2.3.0 -i http://pypi.douban.com/simple --trusted-host pypi.douban.com
  
  pip install py4j -i http://pypi.douban.com/simple --trusted-host pypi.douban.com
  
  pip install pyarrow -i http://pypi.douban.com/simple --trusted-host pypi.douban.com
  
  或
  
  cp -r /opt/cloudera/parcels/SPARK2-2.3.0.cloudera2-1.cdh5.13.3.p0.316101/lib/spark2/python/pyspark /usr/lib64/python2.7/site-packages
  
  yum -y install epel-release
  
  yum install -y python-pip
  
  pip install py4j==0.10.7 -i http://pypi.douban.com/simple --trusted-host pypi.douban.com
  
## 相关资料

  更多详细API请访问 
  
  https://www.pypandas.cn/ 
  
  http://spark.apache.org/docs/latest/api/python/index.html
  
  https://blog.csdn.net/sinat_26917383/article/details/80500349