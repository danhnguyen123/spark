# from pyspark.sql import SparkSession
from pyspark.sql import Row


# spark = SparkSession.builder \
#     .master("local") \
#     .appName("Sparl SQL application") \
#     .getOrCreate()

# A row in DataFrame. The fields in it can be accessed:
#       like attributes (row.key)
#       like dictionary values (row[key])

# key in row will search through row keys.

# Row can be used to create a row object by using named arguments. 
#       It is not allowed to omit a named argument to represent that the value is None or missing. 
#       This should be explicitly set to None in this case.


############## Create DF from list
row = Row(name="Alice", age=11)
print("row =====>", row)
print("row['name'] =====>", row['name'])
print("row['age'] =====>", row['age'])
print("key \"name\" in Row =====>", 'name' in row)
print("key \"wrong_key\" in Row =====>", 'wrong_key' in row)


############## Row also can be used to create another Row like class, 
#               then it could be used to create Row objects, such as:
Person = Row("name", "age")
newPerson = Person("Alice", 11)
print("newPerson =====>", newPerson)


##############  Method
# Return as a dict
row_dict = row.asDict()


