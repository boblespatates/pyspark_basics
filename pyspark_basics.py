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

###########################################

# To deal with data frame, use select methods
df.select('age').show()

# It is possible to pass a list of columns called by name 
df.select(['age','name']).show()

# To deal with object Column
df['age']

# To deal with object Row
df.head(2)

# Add a new column
df.withColumn('double_age', df['age']*2).show()

# Rename a column
df.withColumnRenamed('age','my_new_age').show()

# A bit of sql
df.createOrReplaceTempView('people')
results = spark.sql("SELECT * FROM people")

results.show()

new_results = spark.sql("SELECT * FROM people WHERE age=30")

new_results.show()


