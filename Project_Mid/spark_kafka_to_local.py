from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Create spark session
spark = SparkSession.builder \
    .appName("Kafka To HDFS") \
    .getOrCreate()
spark.sparkContext.setLogLevel('WARN')


# Create dataframe from kafka data
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "18.211.252.152:9092") \
    .option("subscribe","de-capstone3") \
    .option("startingOffsets","earliest") \
    .load()
df.printSchema()
# Transfrom dataframe by dropping few columns and changing value column data type

df = df.withColumn('value_str', df['value'].cast('string').alias('key_str')).drop('value') \
        .drop('key','topic','partition','offset','timestamp','timestampType')

# writing the dataframe to hdfs directory and keep it running until terminated
df1=df.writeStream \
  .outputMode("append") \
  .format("json") \
  .option("truncate","false") \
  .option("path", "/home/hadoop/clickstream_data") \
  .option("checkpointLocation","/home/hadoop/clickstream_checkpoint") \
  .start()

console_df = df \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .trigger(processingTime="1 minute")\
    .start()

console_df.awaitTermination()
df1.awaitTermination()
