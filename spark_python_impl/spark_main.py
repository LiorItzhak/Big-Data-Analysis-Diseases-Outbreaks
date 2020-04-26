import findspark
from pyspark.sql import SparkSession
import os
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
import pandas as pd
import json

findspark.init()
findspark.find()

topic_name = 'atest_13'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 pyspark-shell'

spark = SparkSession.builder \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.sql.execution.arrow.enabled", "true") \
    .master("local[*]") \
    .appName("DiseaseDataC") \
    .getOrCreate()
#
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("maxOffsetsPerTrigger", "1000") \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .load()

schema = StructType(
    [
        StructField("date", TimestampType()),
        StructField("kw", StringType()),
        StructField("region", StringType()),
        StructField("value", DoubleType()),
        StructField("timestamp", TimestampType()),
    ]
)
tmp_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as json") \
    .select(
    from_json(col='json', schema=schema).getField('date').alias('date'),
    from_json(col='json', schema=schema).getField('kw').alias('kw'),
    from_json(col='json', schema=schema).getField('region').alias('region'),
    from_json(col='json', schema=schema).getField('value').alias('value'),
    from_json(col='json', schema=schema).getField('timestamp').alias('timestamp')
)

tmp_df_window = tmp_df \
    .withColumn('kw', regexp_replace(tmp_df.kw, ' ', '_')) \
    .where(tmp_df.region == 'US') \
    .withWatermark('timestamp', '40 second') \
    .groupBy(window('timestamp', '60 day', '1 day'), 'date', 'region', 'kw') \
    .agg(sum('value').alias('value'))


def handle_batch(batch_df, batch_id):
    min_date = batch_df.agg(min('date')).collect()[0]
    max_date = batch_df.agg(max('date')).collect()[0]
    print(f'batch_id={batch_id} | min_date={min_date} || max_date={max_date}')

    batch_df = batch_df.groupBy('window', 'date', 'region') \
        .pivot(pivot_col='kw').agg(first('value')) \
        .sort('window', 'date') \
        .drop('window')
    # date - region - Virus , Snore .... (20)

    #  check our Scala implementation for one file complete solution
    #  this code in SparkBatchProcessKmeanPCA notebook
    #  normalize - z-score -> pca
    #  create vector
    #  crate model fit and predict


    batch_df.show()
    return batch_df


def handle_batch_send_scala(batch_df, batch_id):
    min_date = batch_df.agg(min('date')).collect()[0]
    max_date = batch_df.agg(max('date')).collect()[0]
    print(f'batch_id={batch_id} | min_date={min_date} || max_date={max_date}')

    # pivot  the collected table by kw and values
    batch_df = batch_df.groupBy('window', 'date', 'region') \
        .pivot(pivot_col='kw').agg(first('value')) \
        .sort('window', 'date').drop('window')

    batch_df = batch_df.select(to_json(struct([batch_df[x] for x in batch_df.columns])).alias("value"))

    batch_df.show()

    batch_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "scala_topic") \
        .save()

    return batch_df


sink = tmp_df_window.writeStream \
    .foreachBatch(handle_batch_send_scala) \
    .outputMode("append") \
    .option("truncate", "false") \
    .start().awaitTermination()



# def handle_batch(batch_df, batch_id):
#     min_date = batch_df.agg(min('date')).collect()[0]
#     max_date = batch_df.agg(max('date')).collect()[0]
#     print(f'batch_id={batch_id} | min_date={min_date} || max_date={max_date}')
#
#     # pivot  the collected table by kw and values
#     batch_df = batch_df.groupBy('window', 'date', 'region') \
#         .pivot(pivot_col='kw').agg(first('sum(value)')) \
#         .sort('window', 'date')
#     batch_df.write \
#         .option("header", "true") \
#         .csv("out.csv")
#
#     return batch_df














# one_day_stream = tmp_df \
# .where(tmp_df.region == 'US') \
# .withWatermark('timestamp', '40 second') \
# .groupBy(window('timestamp', '1 day'), 'date', 'region', 'kw') \
# .agg(sum('value'), F.count('date'))



# dir = 'C:/Users/liori/PycharmProjects/big_data_pytrends_disease/demo'
#
# sink2 = tmp_df_window.coalesce(1).writeStream \
#     .outputMode("append") \
#     .format("parquet") \
#     .option("path", f"{dir}/out/parquet'") \
#     .option("checkpointLocation", f"{dir}/tmp") \
#     .start().awaitTermination()
