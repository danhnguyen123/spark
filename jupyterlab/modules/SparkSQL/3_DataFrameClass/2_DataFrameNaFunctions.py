from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("Sparl SQL application") \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

############## Create DF from list
l = [('Alice', 1), ('Bob', 2), (None, 3), ('Carl', None)]

l_df_schema = spark.createDataFrame(l, ['name', 'age'])
print("l_df_schema:")
l_df_schema.show()


############## dropna(how='any', thresh=None, subset=None): Returns a new DataFrame omitting rows with null values.
l_df_schema.dropna().show()
l_df_schema.dropna(how="all").show()
l_df_schema.dropna(thresh=2).show()
l_df_schema.dropna(subset=["age"]).show()

l_df_schema.na.drop().show()
l_df_schema.na.drop(how="all").show()
l_df_schema.na.drop(thresh=2).show()
l_df_schema.na.drop(subset=["age"]).show()


############## fill: Fill null values
l_df_schema.fillna(50).show()
l_df_schema.fillna({'age': 50, 'name': 'unknown'}).show()   # specify values for columns

l_df_schema.na.fill(50).show()
l_df_schema.na.fill({'age': 50, 'name': 'unknown'}).show()  # specify values for columns


############## replace: Returns a new DataFrame replacing a value with another value
l_df_schema.replace('Alice', 'A').show()
l_df_schema.replace('Alice', None).show()
l_df_schema.replace({'Alice': 'A'}).show()
l_df_schema.replace(['Alice', 'Bob'], ['A', 'B'], 'name').show()

l_df_schema.na.replace('Alice', 'A').show()
l_df_schema.na.replace('Alice', None).show()
l_df_schema.na.replace({'Alice': 'A'}).show()
l_df_schema.na.replace(['Alice', 'Bob'], ['A', 'B'], 'name').show()






