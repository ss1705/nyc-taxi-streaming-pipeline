from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, round as spark_round,
    hour, dayofweek, unix_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)
import duckdb
import os

#Spark session
spark = SparkSession.builder \
    .appName("TaxiTripConsumer") \
    .master("local[*]") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

#Schema -- define everything as StringType first bc JSON values from Kafka
#arrive as raw strings; we cast to proper types later
schema = StructType([
    StructField("vendor_id",         StringType(),  True),
    StructField("pickup_datetime",   StringType(),  True),
    StructField("dropoff_datetime",  StringType(),  True),
    StructField("passenger_count",   StringType(),  True),
    StructField("trip_distance",     StringType(),  True),
    StructField("pickup_longitude",  StringType(),  True),
    StructField("pickup_latitude",   StringType(),  True),
    StructField("dropoff_longitude", StringType(),  True),
    StructField("dropoff_latitude",  StringType(),  True),
    StructField("payment_type",      StringType(),  True),
    StructField("fare_amount",       StringType(),  True),
    StructField("tip_amount",        StringType(),  True),
    StructField("total_amount",      StringType(),  True),
    StructField("rate_code",         StringType(),  True),
])

#Read from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "taxi-trips") \
    .option("startingOffsets", "earliest") \
    .load()

#Parse JSON bytes into cols
parsed = raw_stream \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

#Clean and transform
transformed = parsed \
    .filter(
        (col("pickup_datetime").isNotNull()) &
        (col("fare_amount").isNotNull()) &
        (col("trip_distance").cast("double") > 0) &
        (col("pickup_latitude").cast("double") != 0)
    ) \
    .withColumn("pickup_datetime",  to_timestamp("pickup_datetime")) \
    .withColumn("dropoff_datetime", to_timestamp("dropoff_datetime")) \
    .withColumn("fare_amount",      col("fare_amount").cast("double")) \
    .withColumn("tip_amount",       col("tip_amount").cast("double")) \
    .withColumn("total_amount",     col("total_amount").cast("double")) \
    .withColumn("trip_distance",    col("trip_distance").cast("double")) \
    .withColumn("passenger_count",  col("passenger_count").cast("int")) \
    .withColumn("trip_duration_min",
        spark_round(
            (unix_timestamp("dropoff_datetime") -
             unix_timestamp("pickup_datetime")) / 60, 2
        )
    ) \
    .withColumn("pickup_hour",      hour("pickup_datetime")) \
    .withColumn("pickup_dayofweek", dayofweek("pickup_datetime")) \
    .filter(
        (col("fare_amount") > 0) &
        (col("trip_duration_min") > 0) &
        (col("trip_duration_min") < 180)
    )

#DuckDB sink
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "warehouse.duckdb")

def write_to_duckdb(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: empty, skipping.")
        return

    # convert Spark batch to Pandas then write to DuckDB
    pandas_df = batch_df.toPandas()

    conn = duckdb.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_trips (
            vendor_id         VARCHAR,
            pickup_datetime   TIMESTAMP,
            dropoff_datetime  TIMESTAMP,
            passenger_count   INTEGER,
            trip_distance     DOUBLE,
            pickup_longitude  VARCHAR,
            pickup_latitude   VARCHAR,
            dropoff_longitude VARCHAR,
            dropoff_latitude  VARCHAR,
            payment_type      VARCHAR,
            fare_amount       DOUBLE,
            tip_amount        DOUBLE,
            total_amount      DOUBLE,
            rate_code         VARCHAR,
            trip_duration_min DOUBLE,
            pickup_hour       INTEGER,
            pickup_dayofweek  INTEGER
        )
    """)
    conn.execute("INSERT INTO raw_trips SELECT * FROM pandas_df")
    count = conn.execute("SELECT COUNT(*) FROM raw_trips").fetchone()[0]
    conn.close()

    print(f"Batch {batch_id}: wrote {len(pandas_df)} rows. "
          f"Total in warehouse: {count} rows.")

#Write stream - foreachBatch
query = transformed.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_duckdb) \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()