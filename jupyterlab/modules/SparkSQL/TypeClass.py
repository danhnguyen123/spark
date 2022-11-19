from pyspark.sql.types import *

# DataType(): Base class for data types.

# BinaryType(): Binary (byte array) data type.
# BooleanType(): Boolean data type.
# ByteType(): Byte data type, i.e.
# DateType(): Date (datetime.date) data type.
# DecimalType([precision, scale]): Decimal (decimal.Decimal) data type.
# DoubleType(): Double data type, representing double precision floats.
# FloatType(): Float data type, representing single precision floats.
# IntegerType(): Int data type, i.e.
# LongType(): Long data type, i.e.
# ShortType(): Short data type, i.e.
# StringType(): String data type.
# TimestampType(): Timestamp (datetime.datetime) data type.

# ArrayType(elementType, containsNull=True): Array data type.
# Ex: ArrayType(StringType())

# StructField(name, dataType, nullable): A field in StructType.
# StructType([fields]): Struct type, consisting of a list of StructField.


############## Create StructType
struct1 = StructType([StructField("f1", StringType(), True)])
print("struct1[\"f1\"] =====>", struct1["f1"])
print("struct1[0] =====>", struct1[0])

struct2 = StructType([StructField("f1", StringType(), True),
                    StructField("f2", IntegerType(), False)])
print("struct2[\"f2\"] =====>", struct2["f2"])
print("struct2[0] =====>", struct2[0])
print("struct2[0] =====>", struct2[1])


############## Method add(): Construct a StructType by adding new elements to it, to define the schema.
struct1 = StructType().add("f1", StringType(), True)
struct1 = StructType().add(StructField("f1", StringType(), True))
struct1 = StructType().add("f1", "string", True)


############## Method fieldNames(): Returns all field names in a list.
struct = StructType().add("f1", StringType(), True).add("f2", StringType(), True, None)
print("fieldNames =====>", struct.fieldNames())

