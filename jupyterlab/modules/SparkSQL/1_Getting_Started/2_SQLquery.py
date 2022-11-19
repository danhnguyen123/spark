from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PySpark Application").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

df = spark.read.json("./data/people.json")

df.createOrReplaceTempView("people")    # life time is spark session

sqlDF = spark.sql("SELECT * FROM people")
# sqlDF = spark.sql("SELECT * FROM people WHERE age=30")
# sqlDF = spark.sql("SELECT * FROM people WHERE name=\"Andy\"")
sqlDF.show()

