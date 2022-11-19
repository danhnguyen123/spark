from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf().setAppName("PySpark Application")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

distFile = sc.textFile("./data/users.txt")
# distFile.foreach(print)

rdd_collect = distFile.collect()
print("Value =====>", rdd_collect)

