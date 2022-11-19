from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PySpark Application").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

df = spark.read.json("./data/people.json")

print("===============================")
print("Raw table show:")
df.show()

print("===============================")
print("Schema:")
df.printSchema()

print("===============================")
print("Select name:")
df.select("name").show()

print("===============================")
print("Select name, age + 1:")
df.select(df['name'], df['age'] + 1).show()

print("===============================")
print("Filter age:")
df.filter(df['age'] > 21).show()

print("===============================")
print("Groupby age:")
df.groupBy("age").count().show()
