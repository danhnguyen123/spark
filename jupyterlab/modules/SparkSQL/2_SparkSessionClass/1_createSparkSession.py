from pyspark.sql import SparkSession

# The entry point to programming Spark with the Dataset and DataFrame API.

# A SparkSession can be used create DataFrame, 
#                       register DataFrame as tables, 
#                       execute SQL over tables, 
#                       cache tables, 
#                       and read parquet files. 

# To create a SparkSession, use the following builder pattern:
spark = SparkSession.builder \
    .master("local") \
    .appName("Sparl SQL application") \
    .getOrCreate()

