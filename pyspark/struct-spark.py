from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.streaming.kafka import KafkaUtils
import json
import sys
import os
from time import sleep

os.environ['PYSPARK_SUBMIT_ARGS'] = ' --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 pyspark-shell'

spark = SparkSession.builder.appName('Spark Structured Streaming').getOrCreate()

df = spark.readStream.format('kafka').option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "twitter").load()

df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
df.withColumn("value", from_json(col('value')))

query = df.writeStream.outputMode("update").format("console").start()

query.awaitTermination()
