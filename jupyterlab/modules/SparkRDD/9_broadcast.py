from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf().setAppName("PySpark Application")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

data = list(range(5))


rdd = sc.parallelize(data)

broadcastVar = sc.broadcast("Horray!!!!")


def do_something(x):
    print("Value: ", broadcastVar.value)
    
rdd.foreach(do_something)








