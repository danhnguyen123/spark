from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf().setAppName("PySpark Application")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

data = list(range(20))
distData = sc.parallelize(data)


############# reduce
# action_value = distData.reduce(lambda a, b: a + b)

############## collect
# action_value = distData.collect()

############## count
# action_value = distData.count()

############## first
# action_value = distData.first()

############## take
# action_value = distData.take(5)

############## takeSample
# action_value = distData.takeSample(False, 5, 1)

############## saveAsTextFile
# action_value = distData.saveAsTextFile("./text_file")

############## countByKey & countByValue
data1 = [1, 1, 2, 4, 5, 6, 7, 7, 7]
distData1 = sc.parallelize(data1)
distData2 = distData1.map(lambda s: (s, 7))
action_value = distData2.countByKey()
# action_value = distData1.countByValue()

# ############## foreach
# action_value = distData.foreach(lambda x: print(x + 5))

print("Value =====>", action_value)