import findspark
findspark.init('/home/nono/spark-2.4.0-bin-hadoop2.7')
import pyspark

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('ops').getOrCreate()

filepath = '/home/nono/Documents/Python-and-Spark-for-Big-Data-master/Spark_DataFrames/'

df = spark.read.csv(filepath + 'appl_stock.csv',inferSchema=True,header=True)
df.printSchema()

### Grab data with filter
### Wrong
#df.filter("Close < 500").select(['Open', 'Close']).show()
### Right
#df.filter(df['Close'] < 500).select(['Open', 'Close']).show()

#df.filter( (df['Close'] < 200) & (df['Open']>200) ).show()

### and not is made with ~ 
#df.filter( (df['Close'] < 200) & ~(df['Open']>200) ).show()

### You can use .collect to take the result
result = df.filter(df['Low'] == 197.16).collect()

row = result[0]

print(row.asDict())
print(row.asDict()['Volume'])
