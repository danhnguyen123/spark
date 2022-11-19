from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PySpark Application").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

df = spark.read.json("./data/people.json")

df.createGlobalTempView("people")   # life time is spark application

spark.sql("SELECT * FROM global_temp.people").show()

spark.newSession().sql("SELECT * FROM global_temp.people").show()