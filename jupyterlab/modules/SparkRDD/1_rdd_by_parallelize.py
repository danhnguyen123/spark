from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf().setAppName("PySpark Application").setMaster("local")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

data = list(range(24))
distData = sc.parallelize(data, 12)
# distData = sc.parallelize(data, 30)   # should not 


print("===============================")
print("number partition:", distData.getNumPartitions())
print("distData:", distData)










