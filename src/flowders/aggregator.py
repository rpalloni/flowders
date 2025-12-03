import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

APP_NAME = 'OrdersAggregator'
KAFKA_TOPIC = 'orders'
KAFKA_BROKER = 'kafka1:9092,kafka2:9092,kafka3:9092'

schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", TimestampType(), True),
])

def initialize_spark_session(app_name):
    """
    Initialize the Spark Session with provided configurations.

    :param app_name: Name of the spark application.
    """
    try:
        spark = ( 
            SparkSession
            .builder
            .appName(app_name)
            .master('spark://spark-master:7077')
            .config('spark.cores.max', 8)
            .config('spark.executor.cores', 2) # 8/2 4 executors, 2 per worker
            .config('spark.executor.memory', '512M')
            .config('spark.sql.shuffle.partitions', 4) # default shuffle partition 200 not needed - slows down job exec
            .config('spark.streaming.stopGracefullyOnShutdown', True)
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        logging.info('Spark session initialized successfully')
        return spark

    except Exception as e:
        logging.error(f"Spark session initialization failed. Error: {e}")
        return None


def get_streaming_dataframe(spark, brokers, topic):
    """
    Get a streaming dataframe from Kafka.

    :param spark: Initialized Spark session.
    :param brokers: Comma-separated list of Kafka brokers.
    :param topic: Kafka topic to subscribe to.
    :return: Dataframe object or None if there's an error.

    """
    logging.info("Fatching streaming...")
    try:
        # MicroBatchScan step in UI
        kafka_stream = (
            spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", brokers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest") # read from the oldest message in the topic
            .option("failOnDataLoss", "false") # do not fail query in case of data loss
            .option("mode", "PERMISSIVE") # handle possible malformed records gracefully
            #.option("trigger", "") # no trigger defined, so Spark uses ASAP mode
            #.option("maxOffsetsPerTrigger",500) # define micro-batch size
            #.option("truncate", False)
            .load()
        )

        # string_df = kafka_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        
        # Deserialize the key and value
        kafka_stream = kafka_stream.select(
            col("key").cast("string").alias("key"),  # Decode key from bytes to string
            col("value").cast("string").alias("value")  # Decode value from bytes to string
        )
        

        # Parse the JSON value into a structured format
        parsed_stream = kafka_stream.select(
            col("key"),
            col("value"),
            from_json(col("value"), schema).alias("data")
        ).select(
            col("key"),
            col("data.*")  # Flatten the JSON structure
        ).withColumn(
            'event_time', col('timestamp').cast(TimestampType())
        )

        # Aggregation implies data shuffling (Exchange step in UI)
        aggregate = (
            parsed_stream
            .withWatermark('event_time', '1 minute')
            .groupBy(window(col('event_time'), '1 minute'), col('customer_id'))
            #.groupBy(window(col('event_time'), '1 minute', '30 seconds'), col('customer_id')) # sliding window
            .agg(
                count('order_id').alias('client_orders'),
                _sum('amount').alias('client_amount')
            )
        )

        #return parsed_stream # debugging
        return aggregate

    except Exception as e:
        logging.warning(f"Failed to fetch streaming dataframe. Error: {e}")
        return None


def write_streaming(dataframe):
    logging.info("Writing dataframe...")
    query = (
        dataframe
        .writeStream
        .option("truncate", "false")
        .option("checkpointLocation", "checkpoint_repo_kafka")
        .outputMode("complete") # complete for cumulative output, append for last minute output
        .format("console")  # Write to console for debugging
        .start()
    )
    return query


if __name__ == '__main__':
    spark = initialize_spark_session(APP_NAME)
    if spark:
        df = get_streaming_dataframe(spark, KAFKA_BROKER, KAFKA_TOPIC)
        print(df)
        logging.info("Writing dataframe...")
        query = write_streaming(df)
        query.awaitTermination()
    else:
        logging.error("no spark session!!!")