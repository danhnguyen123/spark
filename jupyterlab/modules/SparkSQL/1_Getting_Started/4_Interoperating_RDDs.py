from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField

spark = SparkSession.builder.appName("PySpark Application").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

lines = sc.textFile("./data/people.txt").cache()
parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: (p[0], p[1].strip()))

schemaString = "name age"

# fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
fields = [StructField("name", StringType(), True), 
        StructField("age", StringType(), True)]

schema = StructType(fields)

PeopleDF = spark.createDataFrame(people, schema)

PeopleDF.show()

# PeopleDF.createOrReplaceTempView("people")

# results = spark.sql("SELECT * FROM people")

# results.show()
