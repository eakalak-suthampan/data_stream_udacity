import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# TODO Create a schema for incoming resources
schema = StructType([StructField("crime_id", StringType(), True),
                     StructField("original_crime_type_name", StringType(), True),
                     StructField("report_date", DateType(), True),
                     StructField("call_date", DateType(), True),
                     StructField("offense_date", DateType(), True),
                     StructField("call_time", StringType(), True),
                     StructField("call_date_time", TimestampType(), True),
                     StructField("disposition", StringType(), True)                     
])


def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "calls-for-service") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 200) \
        .option("maxRatePerPartition", 100) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()    

    # Show schema for the incoming resources for checks
    df.printSchema()
    

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("string(value)")
    
    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")
    
    # TODO select original_crime_type_name and disposition
    distinct_table = service_table.selectExpr("original_crime_type_name", "disposition", "call_date_time")
    distinct_table.printSchema()
    
    # count the number of original crime type
    agg_df = distinct_table \
            .withWatermark("call_date_time", "1 days ") \
            .groupBy(psf.window(distinct_table.call_date_time,"1 days","1 hours").alias("TimeFrame"), distinct_table.original_crime_type_name) \
            .count().orderBy(["TimeFrame","count"],ascending=False)    
    
    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
#     query = agg_df \
#             .writeStream \
#             .trigger(processingTime="60 seconds") \
#             .outputMode('Complete') \
#             .format('console') \
#             .option("truncate", "false") \
#             .start()    


#     # TODO attach a ProgressReporter
#     query.awaitTermination()

    # TODO get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath, multiLine=True)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")
    radio_code_df.printSchema()
    # TODO join on disposition column
    join_query = distinct_table \
                 .withWatermark("call_date_time", "1 days ") \
                 .groupBy(psf.window(distinct_table.call_date_time,"1 days","1 hours").alias("TimeFrame"), distinct_table.disposition) \
                 .count().orderBy(["TimeFrame","count"],ascending=False) \
                 .join(radio_code_df, "disposition") \
                 .select("TimeFrame","disposition","description","count") \
                 .writeStream \
                 .trigger(processingTime="60 seconds") \
                 .outputMode('Complete') \
                 .format('console') \
                 .option("truncate", "false") \
                 .start()    

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config("spark.ui.port", 3000) \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
