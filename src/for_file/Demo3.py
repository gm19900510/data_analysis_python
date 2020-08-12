# -*- coding: utf-8 -*-
import json
import pandas as pd

# 所需字段和新老字段映射关系
columns_json_str = '{"name":"影片名称","box_office":"票房"}'
columns_dict = json.loads(columns_json_str)

# 读取本地文件
dataset = pd.read_excel('../data/movies.xls', header=0, encoding='utf-8')

# 获取列的数据类型
print(dataset.dtypes)

# 获取xls文件的列名
print(dataset.columns.values)

# 选取指定列读取到DataFrame
df = pd.DataFrame(dataset, columns=columns_dict.keys())

# 字段重命名
# df.rename(columns={'name':'NEW_NAME', 'src':'NEW_SRC'}, inplace=True) 
df.rename(columns=columns_dict, inplace=True) 
print(df)

# 将重命名之后的数据写入到文件
filepath = '../data/new_movies.xls'
df.to_excel(filepath , header=False, index=0) 

