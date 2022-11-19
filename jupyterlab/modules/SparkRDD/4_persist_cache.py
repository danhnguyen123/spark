from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import StorageLevel

conf = SparkConf().setAppName("PySpark Application")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

lines = sc.textFile("./data/users.txt")
lineLengths = lines.map(lambda s: len(s))

# storage level: MEMORY_ONLY, MEMORY_ONLY_2, MEMORY_AND_DISK, MEMORY_AND_DISK_2, 
#                   DISK_ONLY, DISK_ONLY_2
lineLengths = lines.map(lambda s: len(s)).persist(StorageLevel.MEMORY_ONLY)

# # a shorthand for using the default storage level, which is StorageLevel.MEMORY_ONLY
# lineLengths = lines.map(lambda s: len(s)).cache() 

totalLength = lineLengths.reduce(lambda a, b: a + b)
minLength = lineLengths.reduce(lambda a, b: a if a<=b else b)


print("===============================")
print("[totalLength]:", totalLength)
print("[minLength]:", minLength)