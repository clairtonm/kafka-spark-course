from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import sys
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = ' --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 pyspark-shell'

sc = SparkContext(appName='Spark Streaming')
ssc = StreamingContext(sc, 1)

sc.setLogLevel("WARN")

directKafkaStream = KafkaUtils.createDirectStream(ssc, ["twitter"], {"metadata.broker.list": "localhost:9092", "zookeeper.connection.timeout.ms": "10000"})

directKafkaStream.pprint()

ssc.start()
ssc.awaitTermination()