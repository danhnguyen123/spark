from pyspark.sql import SparkSession
import pandas

spark = SparkSession.builder \
    .master("local") \
    .appName("Sparl SQL application") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

# SparkSession.createDataFrame(data, schema=None, samplingRatio=None, verifySchema=True)

# Creates a DataFrame from an RDD, a list or a pandas.DataFrame.

# When schema is a list of column names, the type of each column will be inferred from data.
# When schema is None, it will try to infer the schema (column names and types) from data, 
#       which should be an RDD of either Row, namedtuple, or dict.
# When schema is pyspark.sql.types.DataType or a datatype string, it must match the real data, 
#       or an exception will be thrown at runtime. 
#       If the given schema is not pyspark.sql.types.StructType, 
#           it will be wrapped into a pyspark.sql.types.StructType as its only field, 
#           and the field name will be “value”. Each record will also be wrapped into a tuple, 
#           which can be converted to row later.


############## Create DF from list
l = [('Alice', 1)]
l_df_non_schema = spark.createDataFrame(l)
print("l_df_non_schema =====>")
l_df_non_schema.show()

l_df_schema = spark.createDataFrame(l, ['name', 'age'])
print("l_df_schema =====>")
l_df_schema.show()


############## Create DF from list of dict
d = [{'name': 'Alice', 'age': 1}, {'name': 'Bob', 'age': 2}]
d_df = spark.createDataFrame(d)
print("d_df =====>")
d_df.show()


############## Create DF from rdd
sc = spark.sparkContext
l = [('Alice', 1)]
rdd = sc.parallelize(l)

rdd_df_non_schema = spark.createDataFrame(rdd)
print("rdd_df_non_schema =====>")
rdd_df_non_schema.show()

rdd_df_schema = spark.createDataFrame(rdd, ['name', 'age'])
print("rdd_df_schema =====>")
rdd_df_schema.show()


############## Create DF from Row
from pyspark.sql import Row
sc = spark.sparkContext
l = [('Alice', 1)]
rdd = sc.parallelize(l)
Person = Row('name', 'age')
person = rdd.map(lambda r: Person(*r))  # unpack r
r_df = spark.createDataFrame(person)
print("r_df =====>")
r_df.show()


############## Create DF from StructType
from pyspark.sql.types import *
schema = StructType([StructField("name", StringType(), True),
                     StructField("age", IntegerType(), True)])
df3 = spark.createDataFrame(rdd, schema)
print("df3 =====>")
df3.show()


############## Create DF from Pandas dataframe
pd_df = spark.createDataFrame(pandas.DataFrame([[1, 2]]))
print("pd_df =====>")
pd_df.show()









