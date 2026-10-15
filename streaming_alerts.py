import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from configs import ALERTS_TOPIC, BOOTSTRAP_SERVERS, CONDITIONS_FILE, SENSORS_TOPIC


def select_alerts(averages, conditions):
    humidity_unused = (F.col("humidity_min") == -999) & (F.col("humidity_max") == -999)
    temperature_unused = (F.col("temperature_min") == -999) & (F.col("temperature_max") == -999)
    humidity_matches = humidity_unused | F.col("humidity_avg").between(
        F.col("humidity_min"), F.col("humidity_max")
    )
    temperature_matches = temperature_unused | F.col("temperature_avg").between(
        F.col("temperature_min"), F.col("temperature_max")
    )
    return (
        averages.crossJoin(F.broadcast(conditions))
        .filter(humidity_matches & temperature_matches)
        .select("window", "temperature_avg", "humidity_avg", "event_count",
                F.col("id").alias("condition_id"), "code", "message")
    )


def main():
    parser = argparse.ArgumentParser(description="Final window averages and CSV-based Kafka alerts")
    parser.add_argument("--checkpoint", default="checkpoints/sensor-alerts")
    parser.add_argument("--run-seconds", type=int, default=0, help="0 means run until Ctrl+C")
    args = parser.parse_args()
    spark = (
        SparkSession.builder.appName("BuildingSensorAlerts")
        .master("local[2]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    sensor_schema = StructType([
        StructField("id", StringType()),
        StructField("temperature", DoubleType()),
        StructField("humidity", DoubleType()),
        StructField("timestamp", DoubleType()),
    ])
    conditions_schema = StructType([
        StructField("id", IntegerType()),
        StructField("humidity_min", DoubleType()),
        StructField("humidity_max", DoubleType()),
        StructField("temperature_min", DoubleType()),
        StructField("temperature_max", DoubleType()),
        StructField("code", IntegerType()),
        StructField("message", StringType()),
    ])
    conditions = spark.read.schema(conditions_schema).option("header", True).csv(str(CONDITIONS_FILE))
    print("CSV alert conditions", flush=True)
    conditions.show(truncate=False)
    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", ",".join(BOOTSTRAP_SERVERS))
        .option("subscribe", SENSORS_TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 1000)
        .load()
    )
    events = (
        source.select(F.from_json(F.col("value").cast("string"), sensor_schema).alias("data"))
        .select("data.*")
        .filter("id IS NOT NULL AND timestamp IS NOT NULL AND temperature IS NOT NULL AND humidity IS NOT NULL")
        .withColumn("event_time", F.col("timestamp").cast("timestamp"))
    )
    averages = (
        events.withWatermark("event_time", "10 seconds")
        .groupBy(F.window("event_time", "1 minute", "30 seconds"))
        .agg(F.avg("temperature").alias("temperature_avg"),
             F.avg("humidity").alias("humidity_avg"),
             F.count("*").alias("event_count"))
    )

    def write_batch(batch, batch_id):
        # In append mode these windows are final: watermark has passed their end.
        batch.persist()
        alerts = select_alerts(batch, conditions).persist()
        try:
            for row in batch.toJSON().collect():
                print(json.dumps({"event": "WINDOW_FINAL", "batch_id": batch_id,
                                  "data": json.loads(row)}), flush=True)
            output = alerts.withColumn("timestamp", F.current_timestamp())
            kafka_output = output.select(
                F.concat_ws(":", F.col("window.start").cast("string"),
                            F.col("window.end").cast("string"), F.col("code")).alias("key"),
                F.to_json(F.struct("*")).alias("value"),
            )
            kafka_output.write.format("kafka").option(
                "kafka.bootstrap.servers", ",".join(BOOTSTRAP_SERVERS)
            ).option("topic", ALERTS_TOPIC).save()
            for row in alerts.toJSON().collect():
                print(json.dumps({"event": "ALERT_WRITTEN", "batch_id": batch_id,
                                  "topic": ALERTS_TOPIC, "data": json.loads(row)}), flush=True)
        finally:
            alerts.unpersist()
            batch.unpersist()

    query = (
        averages.writeStream.outputMode("append")
        .trigger(processingTime="5 seconds")
        .option("checkpointLocation", str(Path(args.checkpoint).resolve()))
        .foreachBatch(write_batch)
        .start()
    )
    print(f"STREAM_STARTED source={SENSORS_TOPIC} output={ALERTS_TOPIC}", flush=True)
    try:
        if args.run_seconds:
            query.awaitTermination(args.run_seconds)
            # Allow the current microbatch to finish before stopping the query.
            query.processAllAvailable()
        else:
            query.awaitTermination()
    except KeyboardInterrupt:
        print("Streaming stopped", flush=True)
    finally:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
