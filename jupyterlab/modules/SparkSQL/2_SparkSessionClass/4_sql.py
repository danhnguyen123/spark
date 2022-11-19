from pyspark.sql import SparkSession
import pandas

spark = SparkSession.builder \
    .master("local") \
    .appName("Sparl SQL application") \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

# SparkSession.sql(sqlQuery)
# Returns a DataFrame representing the result of the given query. 

d = [{'name': 'Alice', 'age': 1}, {'name': 'Bob', 'age': 2}]
d_df = spark.createDataFrame(d)

d_df.createOrReplaceTempView("table1")
df2 = spark.sql("SELECT name AS f1, age as f2 from table1")
df2.show()