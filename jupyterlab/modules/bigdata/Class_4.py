#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import when
from pyspark.sql.functions import col
from pyspark.sql.types import * 
from pyspark.sql.functions import lit
import pyodbc
import pandas as pd
import pyspark.sql.functions as sf


# In[2]:


spark = SparkSession.builder.config("spark.driver.memory", "15g").getOrCreate()


# In[57]:


def process_log_data(path,file_name,date):
    df = spark.read.json(path+file_name)
    df = df.select('_source.AppName','_source.Contract','_source.Mac','_source.TotalDuration')
    df = df.withColumn('Date',lit(date))
    df = df.withColumn('Type',
                 when((col("AppName") == 'CHANNEL') | (col("AppName") =='DSHD')| (col("AppName") =='KPLUS')| (col("AppName") =='KPlus'), "Truyền Hình")
          .when((col("AppName") == 'VOD') | (col("AppName") =='FIMS_RES')| (col("AppName") =='BHD_RES')| 
                 (col("AppName") =='VOD_RES')| (col("AppName") =='FIMS')| (col("AppName") =='BHD')| (col("AppName") =='DANET'), "Phim Truyện")
          .when((col("AppName") == 'RELAX'), "Giải Trí")
          .when((col("AppName") == 'CHILD'), "Thiếu Nhi")
          .when((col("AppName") == 'SPORT'), "Thể Thao")
          .otherwise("Error"))
    df = df.select('Contract','Type','TotalDuration','Date')
    df = df.groupBy('Contract','Type','Date').agg({'TotalDuration':'sum'}).withColumnRenamed('sum(TotalDuration)','TotalDuration')
    return df 


# In[ ]:


def main_task():
    path = 'C:\\Users\\ASUS\\OneDrive\\Big_Data_Analytics\\Dataset\\'
    file_name = '20220401.json'
    date = '2022-04-01'
    df = process_log_data(path,file_name,date)
    i = 2 
    while i < 10:
        file_name = '2022040{}.json'.format(i)
        date = '2022-04-0{}'.format(i)
        df1 = process_log_data(path,file_name,date)
        df = df.union(df1)
        i+= 1 
    while i <= 30: 
        file_name = '202204{}.json'.format(i)
        date = '2022-04-{}'.format(i)
        df1 = process_log_data(path,file_name,date)
        df = df.union(df1)
        i += 1 
        return df 


# In[ ]:


df = main_task() 
df = df.cache()


# In[ ]:


df.show()

