import findspark
findspark.init('/home/nono/spark-2.4.0-bin-hadoop2.7')
import pyspark

from pyspark.sql import SparkSession

filepath = '/home/nono/Documents/Python-and-Spark-for-Big-Data-master/Spark_DataFrames/'

spark = SparkSession.builder.appName('dates').getOrCreate()

df = spark.read.csv(filepath + 'appl_stock.csv',
                    inferSchema=True, header=True)

df.show()
df.printSchema()

from pyspark.sql.functions import (dayofmonth, hour,
                                    dayofyear, month,
                                    year, weekofyear,
                                    format_number, date_format)

df.select(df['Date']).show()
df.select(dayofmonth(df['Date'])).show()
df.select(hour(df['Date'])).show()

df.select(year(df['Date'])).show()

newdf = df.withColumn("Year", year(df['Date']))


# Give the average of something per year
result = newdf.groupBy("Year").mean().select(["Year","avg(Close)"])

new = result.withColumnRenamed("avg(Close)","Average Closing Price")

new.select(['Year',format_number('Average Closing Price',2)]).show()
