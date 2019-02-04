import findspark
findspark.init('/home/nono/spark-2.4.0-bin-hadoop2.7')
import pyspark

from pyspark.sql import SparkSession

filepath = '/home/nono/Documents/Python-and-Spark-for-Big-Data-master/Spark_DataFrames/'

spark = SparkSession.builder.appName('miss').getOrCreate()

df = spark.read.csv(filepath + 'ContainsNull.csv',
                    inferSchema=True, header=True)

df.show()
df.printSchema()

## Select only row
## without missing values
df.na.drop().show()

# they have at least 2 non null values
## to be plotted
df.na.drop(thresh=2).show()

# if you have 'all' or 'any' values
## they you can filter
df.na.drop(how='all').show()


# Drop if there is a null value
## on a specific column
df.na.drop(subset=['Sales']).show()


# Fill the data frame
## replace string by string value by values
df.na.fill('FILL VALUE').show()

# Fill a specific column
df.na.fill('No Name', subset=['Name']).show()

###### Fill  a column with the average value ########
from pyspark.sql.functions import mean

mean_val = df.select(mean(df['Sales'])).collect()
mean_sales = mean_val[0][0]
df.na.fill(mean_sales,['Sales']).show()

##################

# Same things in one line
## ugly and not readable
df.na.fill(df.select(mean(df['Sales'])).collect()[0][0],['Sales']).show()
