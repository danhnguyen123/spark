from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf().setAppName("PySpark Application")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

data = list(range(5))
distData = sc.parallelize(data)
mapData = distData.map(lambda s: s + 1)

def foo(x):
    x = x * 2
    print("Value =====>", x)

mapData.foreach(foo)
# mapData.foreach(print)
