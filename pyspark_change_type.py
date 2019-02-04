import findspark
findspark.init('/home/nono/spark-2.4.0-bin-hadoop2.7')
import pyspark

from pyspark.sql import SparkSession
# For redefining the data type
from pyspark.sql.types import StructField, StringType, IntegerType, StructType

# In order to work with dataframes you need a spark session
spark = SparkSession.builder.appName('Basics').getOrCreate()

filepath = '/home/nono/Documents/Python-and-Spark-for-Big-Data-master/Spark_DataFrames/'

filename = 'people.json'

# Load data
df = spark.read.json(filepath + filename)

df.show()

# Type of data
df.printSchema()

# Column name
df.columns

# General statistics
df.describe().show()

####### Data type redefinition ########

data_schema = [StructField('age', IntegerType(), True),
                StructField('name', StringType(), True)]

final_struc = StructType(fields=data_schema)
df = spark.read.json(filepath + filename, schema=final_struc)

df.printSchema()


