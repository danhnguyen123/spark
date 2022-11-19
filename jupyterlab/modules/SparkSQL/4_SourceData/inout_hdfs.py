from pyspark.sql import SparkSession
import pandas

spark = SparkSession.builder \
    .master("local") \
    .appName("Sparl SQL application") \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

# read json
df1 = spark.read.load("hdfs://172.37.12.187:8020/user/user_test/data_sample/people.json", format="json", inferSchema="true")

# read csv
df2 = spark.read.load("hdfs://172.37.12.187:8020/user/user_test/data_sample/people.csv", 
                        format="csv", sep=";", inferSchema="true", header="true")

# read parquet
df3 = spark.read.load("hdfs://172.37.12.187:8020/user/user_test/data_sample/users.parquet", format="parquet")

# read orc
df4 = spark.read.load("hdfs://172.37.12.187:8020/user/user_test/data_sample/users.orc", format="orc")

# read avro (--packages org.apache.spark:spark-avro_2.12:3.1.2)
df5 = spark.read.load("hdfs://172.37.12.187:8020/user/user_test/data_sample/users.avro", format="avro")



