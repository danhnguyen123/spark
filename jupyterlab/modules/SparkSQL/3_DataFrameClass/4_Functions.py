from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

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


l_df_schema_col = l_df_schema.withColumn('age2', l_df_schema.age - 3)
print("l_df_schema_col")
l_df_schema_col.show()


l_df_schema_col = l_df_schema_col.withColumn('age3', abs(l_df_schema_col.age2))
# ceil(), floor(col), concat(*cols), corr(col1, col2), radians(col), sin(col), ...
# log(arg1[, arg2]), log10(col), log1p(col), log2(col), cos(col), asin(), pow(col1, col2)...
# current_date(), date_format(date, format), hour(col), month(col), ...
# degrees(col), length(col), lower(col), ascii(),...
# max(col), mean(col), min(col), ...


# udf(f=None, returnType=StringType): Creates a user defined function (UDF).
#   function: python function if used as a standalone function
#   returnType: the return type of the user-defined function. 
#               The value can be either a pyspark.sql.types.DataType object or a DDL-formatted type string.

def check_name(person_name):
    if person_name=="Alice":
        return True
    else:
        return False

udf_check_name = udf(check_name, BooleanType())
l_df_schema_col = l_df_schema_col.withColumn('check_Alice', udf_check_name(l_df_schema_col.name))

























