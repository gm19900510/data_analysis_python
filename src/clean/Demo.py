# -*- coding: utf-8 -*-
import json
import pandas as pd

# 所需字段和新老字段映射关系
columns_json_str = '{"name":"NEW_NAME","src":"NEW_SRC"}'
columns_dict = json.loads(columns_json_str)

# 读取本地文件
dataset = pd.read_csv('movies.csv', header=0, encoding='utf-8', dtype=str)

# 获取csv文件的列名
print(dataset.columns.values)

df = pd.DataFrame(dataset, columns=columns_dict.keys())

# 字段重命名
# df.rename(columns={'name':'NEW_NAME', 'src':'NEW_SRC'}, inplace=True) 
df.rename(columns=columns_dict, inplace=True) 
print(df)

# 将重命名之后的数据写入到文件
filepath = 'new_movies.csv'
df_columns = pd.DataFrame([list(df.columns)])
df_columns.to_csv(filepath, mode='w', header=False, index=0) 

df.to_csv(filepath, mode='a', header=False, index=0)

