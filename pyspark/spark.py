from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.util import kafka
import json

sc = SparkContext('localhost[*]', 'Spark Streaming')
ssc = StreamingContext(sc, 1)