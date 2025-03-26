#importing libraries

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.functions import unix_timestamp, from_unixtime

#creating Spark Session

spark = SparkSession.builder.appName("datewise_booking").getOrCreate()
sc = spark.sparkContext
sc
#Reading the bookings Data into dataframe
df = spark.read.csv("/home/hadoop/bookings_data/part*",inferSchema = True)

# renaming the columns of the dataframe
df1 = df.withColumnRenamed("_c0","booking_id")\
           .withColumnRenamed("_c1","customer_id") \
           .withColumnRenamed("_c2","driver_id") \
           .withColumnRenamed("_c3","customer_app_version")  \
           .withColumnRenamed("_c4","customer_phone_os_version") \
           .withColumnRenamed("_c5","pickup_lat") \
           .withColumnRenamed("_c6","pickup_lon") \
           .withColumnRenamed("_c7","drop_lat") \
           .withColumnRenamed("_c8","drop_lon") \
           .withColumnRenamed("_c9","pickup_timestamp")  \
           .withColumnRenamed("_c10","drop_timestamp")  \
           .withColumnRenamed("_c11","trip_fare") \
           .withColumnRenamed("_c12","tip_amount")  \
           .withColumnRenamed("_c13","currency_code") \
           .withColumnRenamed("_c14","cab_color")  \
           .withColumnRenamed("_c15","cab_registration_no") \
           .withColumnRenamed("_c16","customer_rating_by_driver")  \
           .withColumnRenamed("_c17","rating_by_customer")  \
           .withColumnRenamed("_c18","passenger_count")
#filtering the dataframe On Date .
df2 =  df1.withColumn("booking_date", date_format('pickup_timestamp', "yyyy-MM-dd"))

df2.show(5)
# Date wise aggregation stored in date.
date = df2.select('booking_date').groupBy('booking_date').count()

date.show(10)
date.count()
date.coalesce(1).write.format('com.databricks.spark.csv').mode('overwrite').save('/home/hadoop/datewise_aggreation', header = 'false')

# convering dataframe in csv format and storing it in hdfs

df1.coalesce(1).write.format('com.databricks.spark.csv').mode('overwrite').save('/home/hadoop/booking_data_csv', header = 'false')