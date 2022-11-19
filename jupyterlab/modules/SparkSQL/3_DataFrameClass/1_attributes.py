from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("Sparl SQL application") \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

############## Create DF from list
l = [('Alice', 1), ('Bob', 2), ('David', 3)]

l_df_schema = spark.createDataFrame(l, ['name', 'age'])
print("l_df_schema =====>")
l_df_schema.show()


# columns: Returns all column names as a list.
print("columns =====>", l_df_schema.columns)

# dtypes: Returns all column names and their data types as a list.
print("dtypes =====>", l_df_schema.dtypes)

# isStreaming: Returns True if this Dataset contains one or more sources that continuously return data as it arrives.
print("isStreaming =====>", l_df_schema.isStreaming)

# rdd: Returns the content as an pyspark.RDD of Row.
print("rdd =====>", l_df_schema.rdd)

# schema: Returns the schema of this DataFrame as a pyspark.sql.types.StructType.
print("schema =====>", l_df_schema.schema)

# na: Returns a DataFrameNaFunctions for handling missing values.
print("na =====>", l_df_schema.na)

# l_df_schema.na.drop(), l_df_schema.na.replace(), l_df_schema.na.fill()

