# Please complete the TODO items below

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def built_in_sink_exercise():
    """
    Make sure to submit this code using spark-submit command rather than running on jupyter notebook
    We'll be using "console" sink
    :return:
    """
    spark = SparkSession.builder \
        .master("local") \
        .appName("spark streaming example") \
        .getOrCreate()

    df = spark.read.json('data.txt')

    # TODO create a struct for this schema - you might want to load this file in jupyter notebook then figure out the schema
    jsonSchema = StructType([StructField("status", StringType(), True),
                             StructField("timestamp", TimestampType(), True)])

    # TODO create a dataframe using this json schema, set maxFilesPerTrigger as 1
    # note - this will only print the progress report once because we ONLY have one file!
    # if you want to see multiple batches in the console, try to duplicate this data.txt file within the directory
    streaming_input_df = spark\
            .readStream\
            .schema(jsonSchema)\
            .option("maxFilesPerTrigger", 1)\
            .json('data.txt')
    
    
    streaming_count_df = streaming_input_df\
            .groupBy(
            streaming_input_df.status,
            window(streaming_input_df.timestamp, "1 hour"))\
            .count()
    

    #TODO check the status of the streaming counts dataframe
    streaming_count_df.isStreaming

    #modify this to have various outputMode
    # TODO use console for now
    query = streaming_count_df\
            .writeStream\
            .format("console")\
            .queryName("counts")\
            .outputMode("complete")\
            .start()

if __name__ == "__main__":
    built_in_sink_exercise()
