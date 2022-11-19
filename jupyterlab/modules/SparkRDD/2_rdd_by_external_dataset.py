from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf().setAppName("PySpark Application")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

distFile = sc.textFile("./data/users.txt")

print("===============================")
print("distFile:", distFile)
