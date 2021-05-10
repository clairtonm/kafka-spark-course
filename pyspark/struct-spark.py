from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
# from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import *
import json
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = ' --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 pyspark-shell'

spark = SparkSession.builder.appName('Spark Structured Streaming').getOrCreate()

df = spark.readStream.format('kafka').option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "twitter").load()

jsonSchema = StructType([StructField("created_at", DoubleType(), True), StructField("hashtags", 
                        ArrayType(StructType([StructField("text", StringType(), True), StructField("indices", ArrayType(IntegerType(), True))])), True),
                        StructField("favorite_count", DoubleType(), True), StructField("retweet_count", DoubleType(), True),
                        StructField("text", StringType(), True), StructField("id", StringType(), True),
                        StructField("geo", StructType([StructField("type", StringType(), True), StructField("coordinates", ArrayType(LongType(), True))]), True), 
                        StructField("lang", StringType(), True)])

query =df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").withColumn("json", from_json(col('value').cast("string"), jsonSchema)).select(col("json.*")).writeStream.outputMode("update").format("console").option("truncate", "false").start()

# df.withColumn("json", from_json(col('value').cast("string"), jsonSchema))

# query = df.select(col("json.*")).writeStream.outputMode("update").format("console").option("truncate", "false").start()

# query = df.select(col("key"), col("value")).writeStream.format("kafka").option("checkpointLocation", "data\checkpoint").option("kafka.bootstrap.servers", "localhost:9092").option("topic", "twitter_result").start()

# df.withColumn("value", from_json(col('value'), jsonSchema))

df.printSchema()

query.awaitTermination()
