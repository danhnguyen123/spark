from pyspark.sql import SparkSession
import pandas

spark = SparkSession.builder \
    .master("local") \
    .appName("Sparl SQL application") \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

# SparkSession.range(start, end=None, step=1, numPartitions=None)
# Create a DataFrame with single pyspark.sql.types.LongType column named id, 
#       containing elements in a range from start to end (exclusive) with step value step.


range_df_1 = spark.range(1, 7, 2)
print("range_df_1 =====>")
range_df_1.show()

range_df_2 = spark.range(3)
print("range_df_2 =====>")
range_df_2.show()