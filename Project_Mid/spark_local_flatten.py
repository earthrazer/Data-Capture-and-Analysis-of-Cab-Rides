
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create spark session
spark = SparkSession.builder \
    .appName("Kafka To HDFS") \
    .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

# Reading json data into dataframe
df = spark.read.json('/home/hadoop/clickstream_data/part*')

# extracting columns from json value in dataframe and create new dataframe with new coloumns
df1 = df.select(
    get_json_object(df["value_str"],"$.customer_id").alias("customer_id"),
    get_json_object(df["value_str"],"$.app_version").alias("app_version"),
    get_json_object(df["value_str"],"$.OS_version").alias("OS_version"),
    get_json_object(df["value_str"],"$.lat").alias("lat"),
    get_json_object(df["value_str"],"$.lon").alias("lon"),
    get_json_object(df["value_str"],"$.page_id").alias("page_id"),
    get_json_object(df["value_str"],"$.button_id").alias("button_id"),
    get_json_object(df["value_str"],"$.is_button_click").alias("is_button_click"),
    get_json_object(df["value_str"],"$.is_page_view").alias("is_page_view"),
    get_json_object(df["value_str"],"$.is_scroll_up").alias("is_scroll_up"),
    get_json_object(df["value_str"],"$.is_scroll_down").alias("is_scroll_down"),
    get_json_object(df["value_str"],"$.timestamp\n").alias("click_timestamp")
)

# print schema of dataframe with new columns
print(df1.schema)

#print 10 records from dataframe
df1.show(10)

# Save dataframe to csv file in hdfs directory
df1.coalesce(1).write.format('com.databricks.spark.csv').mode('overwrite').save('/home/hadoop/clickstream_data_flatten', header = 'false')