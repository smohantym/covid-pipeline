import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp, date_format, coalesce
from pyspark.sql.types import StructType, StructField, StringType, LongType

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "covid_events")
OUT_DIR = os.getenv("SPARK_OUTPUT_DIR", "/opt/spark-output/covid")
CHECKPOINT = os.getenv("SPARK_CHECKPOINT_DIR", "/opt/spark-output/.chk_covid")
DEBUG_CONSOLE = os.getenv("SPARK_DEBUG_CONSOLE", "false").lower() in {"1","true","yes"}
TRIGGER_SEC = int(os.getenv("SPARK_TRIGGER_SEC", "20"))

PARQUET_PATH = os.path.join(OUT_DIR, "parquet")
CSV_PATH = os.path.join(OUT_DIR, "csv")

spark = (
    SparkSession.builder
    .appName("Covid19KafkaToParquet")
    .config("spark.sql.shuffle.partitions", "1")  # fewer files
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("_ingest_ts", StringType()),
    StructField("source_date", StringType()),
    StructField("country", StringType()),
    StructField("country_code", StringType()),
    StructField("slug", StringType()),
    StructField("new_confirmed", LongType()),
    StructField("total_confirmed", LongType()),
    StructField("new_deaths", LongType()),
    StructField("total_deaths", LongType()),
    StructField("new_recovered", LongType()),
    StructField("total_recovered", LongType()),
])

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")   # first run: read all
    .option("failOnDataLoss", "false")
    .load()
)

parsed = raw.select(
    col("key").cast("string").alias("key"),
    from_json(col("value").cast("string"), schema).alias("v")
)

records = parsed.select("key", "v.*")

clean = (
    records
    .withColumn("ingest_ts", to_timestamp(col("_ingest_ts")))
    .withColumn("event_ts", to_timestamp(col("source_date")))
    .withColumn("processing_ts", current_timestamp())
    .withColumn("p_date", date_format(coalesce(col("ingest_ts"), col("processing_ts")), "yyyy-MM-dd"))
)

if DEBUG_CONSOLE:
    (
        clean.writeStream
        .format("console")
        .option("truncate", False)
        .option("numRows", 20)
        .trigger(processingTime=f"{TRIGGER_SEC} seconds")
        .outputMode("append")
        .start()
    )

csv_cols = [
    "country","country_code","event_ts","ingest_ts",
    "new_confirmed","total_confirmed","new_deaths","total_deaths","p_date"
]

def foreach_batch(df, batch_id: int):
    if df.rdd.isEmpty():
        return
    subset = df.select(*csv_cols).cache()

    # Parquet
    (
        subset.coalesce(1)
        .write.mode("append")
        .partitionBy("p_date")
        .parquet(PARQUET_PATH)
    )

    # CSV
    (
        subset.coalesce(1)
        .write.mode("append")
        .option("header", True)
        .partitionBy("p_date")
        .csv(CSV_PATH)
    )
    subset.unpersist()

(
    clean.writeStream
    .foreachBatch(foreach_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime=f"{TRIGGER_SEC} seconds")
    .outputMode("append")
    .start()
    .awaitTermination()
)
