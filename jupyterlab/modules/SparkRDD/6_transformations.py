from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf().setAppName("PySpark Application")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

data = list(range(20))
distData = sc.parallelize(data, 4)

############# map
transform_value = distData.map(lambda s: s + 1)

# ############# filter
# transform_value = distData.filter(lambda s: s > 15)

# # ############# flatMap
# data = ["hello world", "apache spark"]
# distData = sc.parallelize(data)
# transform_value = distData.flatMap(lambda s: s.split())

# ############ mapPartitions
# def foo_partition(partition_data):
#     return_list = []
#     for data_value in partition_data:
#         return_list.append(data_value/10)
#     print("Partition Value =====>", return_list)
#     return return_list

# transform_value = distData.mapPartitions(foo_partition)

# ############# mapPartitionsWithIndex
# def foo_partition(partition_index, partition_data):
#     return_list = []

#     if partition_index == 1:
#         for data_value in partition_data:
#             return_list.append(data_value/10)
#         print("Value =====>", partition_index, return_list)
#     else:
#         for data_value in partition_data:
#             return_list.append(data_value*10)
#         print("Partition Value =====>", partition_index, return_list)
#     return return_list

# transform_value = distData.mapPartitionsWithIndex(foo_partition)

############## sample
# transform_value = distData.sample(False, 0.8, 3)

# ############# union
# data1 = list(range(15, 25))
# distData1 = sc.parallelize(data1, 4)
# transform_value = distData.union(distData1)

############## intersection
# data1 = list(range(15, 25))
# distData1 = sc.parallelize(data1, 4)
# transform_value = distData.intersection(distData1)

############## distinct
# data1 = [1, 1, 2, 4, 5, 6, 7, 7, 7]
# distData1 = sc.parallelize(data1, 4)
# transform_value = distData1.distinct()

# ############## groupByKey
# data1 = [1, 1, 2, 4, 5, 6, 7, 7, 7]
# distData1 = sc.parallelize(data1, 4).map(lambda s: (s, 1))
# transform_value = distData1.groupByKey()

# ############## reduceByKey & sortByKey
# data1 = [1, 1, 2, 4, 5, 6, 7, 7, 7]
# distData1 = sc.parallelize(data1, 4).map(lambda s: (s, 1))
# transform_value = distData1.reduceByKey(lambda a, b: a + b).sortByKey()

# ############# repartition
# print("old number partition:", distData.getNumPartitions())
# repartition_rdd = distData.repartition(12)
# print("new number partition:", repartition_rdd.getNumPartitions())



value_collect = transform_value.collect()
print("Value =====>", value_collect)