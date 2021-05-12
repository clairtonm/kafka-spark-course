from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, desc, trim, col, sum, avg, max
from pyspark.sql.types import *
import os

## Download Jars for Spark

os.environ['PYSPARK_SUBMIT_ARGS'] = ' --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 pyspark-shell'

# Initialize Spark Session

spark = SparkSession.builder.appName('Spark Structured Streaming') \
        .getOrCreate()

# Subscribe to Kafka topic
df = spark.readStream.format('kafka').option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "log").load()

schema = StructType([StructField("Date first seen", StringType(), True),
                    StructField("Duration", StringType(), True),
                    StructField("Proto", StringType(), True),
                    StructField("Src IP Addr", StringType(), True),
                    StructField("Src Pt", StringType(), True),
                    StructField("Packets", StringType(), True),
                    StructField("Bytes", StringType(), True),
                    StructField("Flows", StringType(), True),
                    StructField("Flags", StringType(), True),
                    StructField("Tos", StringType(), True),
                    StructField("class", StringType(), True),
                    StructField("attackType", StringType(), True),
                    StructField("attackID", StringType(), True),
                    StructField("attackDescription", StringType(), True)])

df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")\
        .withColumn("json", from_json(col('value').cast('string'), schema))\

# Cast columns
df = df.withColumn("Date first seen", col("json.Date first seen").cast(TimestampType())) \
        .withColumn("Duration", col("json.Duration").cast("double")) \
        .withColumn("Packets", trim(col("json.Packets")).cast(IntegerType()))

# query = df.writeStream \
#             .outputMode("append") \
#             .format("console") \
#             .option("truncate", "false") \
#             .start()

# query.awaitTermination()

kafka_query = df.select(col("key"), col("value")).withColumn("value", col("value").cast(StringType())) \
        .writeStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("checkpointLocation", "kafka_checpoint/") \
        .option("failOnDataLoss", "false") \
        .option("topic", "log-result") \
        .start()

kafka_query.awaitTermination()