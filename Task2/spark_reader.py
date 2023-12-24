from argparse import ArgumentParser, Namespace

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import window
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType

def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument("--input_dir", type=str, default="tg_messages/")
    parser.add_argument("--aggregation_window", type=int, default=180, help="Size of aggregation window in seconds.")
    parser.add_argument("--moving_average_window", type=int, default=10_800, help="Size of moving average window in seconds.")
    parser.add_argument("--anomaly_factor", 
                        type=int, 
                        default=2, 
                        help="The number of messages is considered anomalous if it exceeds the moving average by so many times.")
    parser.add_argument("--debug_mode", action="store_true", help="If set, the resulting data is displayed sorted and in 'complete' mode.")
    return parser.parse_args()

def main():
    args = parse_args()

    spark = SparkSession.builder \
                        .appName("structured") \
                        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", True) \
                        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", False) \
                        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType([
        StructField("timestamp", LongType(), nullable=False),
        StructField("source", StringType(), nullable=False),
        StructField("message", StringType(), nullable=True),
        StructField("is_media_presented", BooleanType(), nullable=False)
    ])

    df = spark.readStream \
              .format("parquet") \
              .option("path", args.input_dir) \
              .schema(schema) \
              .load()
    df = df.withColumn("timestamp", F.from_unixtime(df.timestamp / 1e9).cast("timestamp")) \
           .withWatermark("timestamp", "1 day")
    
    agg_window_size = args.aggregation_window
    mov_avg_window_size = args.moving_average_window

    agg_window = window(df.timestamp, f"{agg_window_size} seconds")
    agg_df = df.groupBy(df.source, agg_window).count()
    agg_df = agg_df.select(agg_df.source, agg_df.window.start.alias("timestamp"), F.col("count").alias("agg_count"))

    mov_avg_window = window(agg_df.timestamp, f"{mov_avg_window_size + agg_window_size} seconds", f"{agg_window_size} seconds")
    mov_avg_df = agg_df.groupBy(agg_df.source, mov_avg_window) \
                       .agg(F.last(agg_df.agg_count).alias("last_window_count"),
                            F.last(agg_df.timestamp).alias("last_window_timestamp"),
                            F.sum(agg_df.agg_count).alias("total_count"),
                            F.count(agg_df.agg_count).alias("windows_count"))
    mov_avg_df = mov_avg_df.select(mov_avg_df.source,
                                   mov_avg_df.last_window_timestamp,
                                   mov_avg_df.window.end.alias("timestamp"),
                                   mov_avg_df.last_window_count,
                                   ((mov_avg_df.total_count - mov_avg_df.last_window_count) / (mov_avg_df.windows_count - 1)).alias("avg_count"))
    mov_avg_df = mov_avg_df.where(F.unix_timestamp(mov_avg_df.timestamp) - F.unix_timestamp(mov_avg_df.last_window_timestamp) < agg_window_size * 2)
    mov_avg_df = mov_avg_df.select(mov_avg_df.source,
                                   mov_avg_df.last_window_timestamp,
                                   mov_avg_df.last_window_count,
                                   mov_avg_df.avg_count)
    
    anomalies = mov_avg_df.where((~mov_avg_df.avg_count.isNull()) & \
                                 (mov_avg_df.last_window_count > mov_avg_df.avg_count * args.anomaly_factor))
    if (args.debug_mode):
        anomalies = anomalies.orderBy(mov_avg_df.source, mov_avg_df.last_window_timestamp)
    
    query = None
    try:
        query = anomalies.writeStream \
                         .outputMode("complete" if args.debug_mode else "update") \
                         .format("console") \
                         .option("truncate", "false") \
                         .start()
        while True:
            pass
    except KeyboardInterrupt:
        pass
    finally:
        if query:
            query.stop()

if __name__ == "__main__":
    main()
