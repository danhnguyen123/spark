from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf().setAppName("PySpark Application")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

data = list(range(24))
rdd = sc.parallelize(data)

# # Wrong: Don't do this!!
# counter = 0
# def increment_counter(x):
#     global counter
#     counter += x
#     print("Counter value: ", counter)

# rdd.foreach(increment_counter)

# print("===============================")
# print("Counter value: ", counter)


# Using accumulator
accum = sc.accumulator(0)

def increment_counter(x):
    # print("Counter value: ", accum.value)
    accum.add(1)

rdd.foreach(increment_counter)

print("===============================")
print("Counter value: ", accum.value)






