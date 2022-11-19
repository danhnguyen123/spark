from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("Sparl SQL application") \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

############## Create DF from list
l = [('Alice', 1), ('Bob', 2), ('David', 3)]

l_df_schema = spark.createDataFrame(l, ['name', 'age'])
print("l_df_schema =====>", l_df_schema.collect())


# show(n=20, truncate=True, vertical=False): Prints the first n rows to the console.
print("l_df_schema_col_show")
l_df_schema.show()
l_df_schema.show(truncate=3)    # truncate all string to specific length
l_df_schema.show(vertical=True)


# printSchema(): Prints out the schema in the tree format.
print("l_df_schema_printschema")
l_df_schema.printSchema()


# cache(): Persists the DataFrame with the default storage level (MEMORY_AND_DISK).
l_df_schema.cache()


# collect(): Returns all the records as a list of Row.
print("l_df_schema_collect =====>", l_df_schema.collect())


# withColumn(colName, col): Returns a new DataFrame by adding a column or replacing the existing column that has the same name.
l_df_schema_col = l_df_schema.withColumn('age2', l_df_schema.age + 1)
print("l_df_schema_col")
l_df_schema_col.show()


# withColumnRenamed(existing, new): Returns a new DataFrame by renaming an existing column.
l_df_schema_recol = l_df_schema.withColumnRenamed('age', 'age2').withColumnRenamed('name', 'name2')
print("l_df_schema_recol")
l_df_schema_recol.show()


# corr(col1, col2[, method]): Calculates the correlation of two columns of a DataFrame as a double value.
l_df_schema_col.corr("age", "age2")


# createTempView(name): Creates a local temporary view with this DataFrame.
l_df_schema.createTempView("people")
df2 = spark.sql("select * from people")

# The lifetime of this temporary table is tied to the SparkSession that was used to create this DataFrame. 
# throws TempTableAlreadyExistsException, if the view name already exists in the catalog.
l_df_schema.createTempView("people")   


# createOrReplaceTempView(name): Creates or replaces a local temporary view with this DataFrame.
l_df_schema.createOrReplaceTempView("people")   


# createGlobalTempView(name): Creates a global temporary view with this DataFrame.
# The lifetime of this temporary view is tied to this Spark application. throws
l_df_schema.createGlobalTempView("people")
df2 = spark.sql("select * from people")

# throws TempTableAlreadyExistsException, if the view name already exists in the catalog.
l_df_schema.createGlobalTempView("people")   


# createOrReplaceGlobalTempView(name): Creates or replaces a global temporary view using the given name.
l_df_schema.createOrReplaceGlobalTempView("people")   


# describe(*cols): Computes basic statistics for numeric and string columns.
l_df_schema.describe(['age']).show()
l_df_schema.describe().show()


# select(*cols): Projects a set of expressions and returns a new DataFrame.
l_df_schema.select('*').show()
l_df_schema_col.select('name', 'age2').show()
l_df_schema.select(l_df_schema.name, (l_df_schema.age + 10).alias('age3')).show()


# crossJoin(other): Returns the cartesian product with another DataFrame.
l_df_schema.crossJoin(l_df_schema_col).show()
l_df_schema.crossJoin(l_df_schema_col.select("age2")).show()


# distinct(): Returns a new DataFrame containing the distinct rows in this DataFrame.
l_df_schema.distinct().show()


# drop(*cols): Returns a new DataFrame that drops the specified column.
l_df_schema.drop('age').show()
l_df_schema.drop(l_df_schema.age).show()
l_df_schema_col.drop('name', 'age2').show()


# dropDuplicates(subset=None): Return a new DataFrame with duplicate rows removed, optionally only considering certain columns.
# alias for drop_duplicates
from pyspark.sql import Row
sc = spark.sparkContext
df = sc.parallelize([ \
    Row(name='Alice', age=5, height=80), \
    Row(name='Alice', age=5, height=80), \
    Row(name='Alice', age=10, height=80)]).toDF()

df.dropDuplicates().show()
df.dropDuplicates(['name', 'height']).show()


# exceptAll(other): Return a new DataFrame containing rows in this DataFrame but not in another DataFrame while preserving duplicates.
df1 = spark.createDataFrame(
        [("a", 1), ("a", 1), ("a", 1), ("a", 2), ("b",  3), ("c", 4)], ["C1", "C2"])
df2 = spark.createDataFrame([("a", 1), ("b", 3)], ["C1", "C2"])
df1.exceptAll(df2).show()
df2.exceptAll(df1).show()


# filter(condition): Filters rows using the given condition.
# where(condition) alias for filter
l_df_schema.filter((l_df_schema.age > 2) and (l_df_schema.age < 3)).show()
l_df_schema.filter("age == 2").show()


# first(), head(n=None), take(num), tail(num): return Row of Dataframe.
l_df_schema.first()

l_df_schema.head()
l_df_schema.head(1)

l_df_schema.take(2)

l_df_schema.tail(1)


# foreach(f): Applies the f function to all Row of this DataFrame.
def f(person):
    print(person.name)

l_df_schema.foreach(f)


# foreachPartition(f): Applies the f function to each partition of this DataFrame.
def f(people):
    # print(people)
    for person in people:
        print(person.name)

l_df_schema.foreachPartition(f)


# groupBy(*cols): Groups the DataFrame using the specified columns, so we can run aggregation on them.
# avg(*cols), count(), max(*cols), mean(*cols), min(*cols), sum(*cols)
df1.groupBy("C1").avg().show()
df1.groupBy("C1").count().show()
df.groupBy("name").agg({"age": "sum", "height": "count"}).show()


# intersect(other): Return a new DataFrame containing rows only in both this DataFrame and another DataFrame.
df1.intersect(df2).show()


# intersectAll(other): Return a new DataFrame containing rows in both this DataFrame and another DataFrame while preserving duplicates.
df1.intersectAll(df2).show()


# join(other, on=None, how=None): Joins with another DataFrame, using the given join expression.
# how: default inner. Must be one of: inner, outer, left, right, ...
l_df_schema.join(l_df_schema_col, 'name', 'outer')

l_df_schema.join(l_df_schema_col, ['name', 'age'],'outer')

cond = [l_df_schema.name == l_df_schema_col.name, l_df_schema.age == l_df_schema_col.age]
l_df_schema.join(l_df_schema_col, cond, 'outer')


# repartition(numPartitions, *cols): Returns a new DataFrame partitioned by the given partitioning expressions. 
# The resulting DataFrame is hash partitioned.
l_df_schema.repartition(10).rdd.getNumPartitions()
l_df_schema.repartition("age")
l_df_schema.repartition(7, "age")
l_df_schema.repartition("name", "age")


# sort(*cols, **kwargs): Returns a new DataFrame sorted by the specified column(s).
# Other Parameters: ascendingbool or list, optional
# Alias for orderBy
l_df_schema.sort("age", ascending=False).show()
df1.sort(["C1", "C2"], ascending=[False, True]).show()

df1.orderBy(["C1", "C2"], ascending=[False, False]).show()


# toPandas(): Returns the contents of this DataFrame as Pandas pandas.DataFrame.
pandas_dataframe = l_df_schema.toPandas()  












