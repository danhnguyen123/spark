from pyspark.sql import SparkSession
import pandas

spark = SparkSession.builder \
    .master("local") \
    .appName("Sparl SQL application") \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

# pyspark.sql.SparkSession.read
# Returns a DataFrameReader that can be used to read data in as a DataFrame.
reader = spark.read



struct2 = StructType([StructField("name", StringType(), True),
                    StructField("age", StringType(), False)])

# read
df = spark.read.load("./data/people.json", format="json", schema=struct2)

df1 = spark.read.format('json').load("./data/people1.json")

df2 = spark.read.json("./data/people2.json")

df3 = spark.read.format('json').load(["./data/people.json","./data/people1.json","./data/people2.json"])



# Query that file directly with SQL.
df4 = spark.sql("SELECT * FROM json.`./data/people2.json`")



# pyspark.sql.DataFrame.write
# Interface for saving the content of the non-streaming DataFrame out into external storage.
writer = df.write

# write
df.write.save("./data/Ages", format="json")

df1.write.format("json").save("./data/Ages1")

df2.write.json("./data/Ages2")

