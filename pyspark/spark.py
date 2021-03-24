from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import sys
import os

print(sys.version)

os.environ['PYSPARK_SUBMIT_ARGS'] = ' --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.0 pyspark-shell'

sc = SparkContext(appName='Spark Streaming')
ssc = StreamingContext(sc, 1)

sc.setLogLevel("WARN")

# kafka_stream = KafkaUtils.createStream(ssc,'localhost:2181','raw-event-streaming-consumer',{'test-topic':1})

directKafkaStream = KafkaUtils.createDirectStream(ssc, ["test-topic"], {"metadata.broker.list": "localhost:9092", "zookeeper.connection.timeout.ms": "10000"})
# kafka_stream.pprint()

directKafkaStream.pprint()

ssc.start()
ssc.awaitTermination()