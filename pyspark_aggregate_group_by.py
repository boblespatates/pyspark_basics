import findspark
findspark.init('/home/nono/spark-2.4.0-bin-hadoop2.7')
import pyspark

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('aggs').getOrCreate()

filepath = '/home/nono/Documents/Python-and-Spark-for-Big-Data-master/Spark_DataFrames/'

df = spark.read.csv(filepath + 'sales_info.csv', inferSchema=True, header=True)

df.show()
df.printSchema()

### Group
df.groupBy("Company").min().show()
df.groupBy("Company").count().show()

### Aggregate
''' You pass the column you want to agregate
as the key. And you pass the function you
want to use as the value '''
df.agg({'Sales':'sum'}).show()
df.agg({'Sales':'max'}).show()

group_data = df.groupBy("Company")
group_data.agg({'Sales':'max'}).show()

# Still using sql commands
from pyspark.sql.functions import countDistinct,avg,stddev

df.select(countDistinct('Sales')).show()
df.select(avg('Sales').alias('Average Sales')).show()

df.select(stddev('Sales')).show()


from pyspark.sql.functions import format_number

sales_std = df.select(stddev("Sales").alias('std'))
''' but you still have the pb that you don't
want so much significant numbers'''

# To adjust the nb of significant nb
sales_std.select(format_number('std',2)).show()


### Sort things

df.orderBy("Sales").show()

## Sort by from most to lowest
df.orderBy(df['Sales'].desc()).show()

