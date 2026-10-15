"""Verify the completed stream against Kafka input using independent arithmetic."""
import csv
import json
import math
from collections import Counter
from datetime import datetime, timezone

from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from configs import ALERTS_TOPIC, BOOTSTRAP_SERVERS, CONDITIONS_FILE, SENSORS_TOPIC
from streaming_alerts import select_alerts


def read_topic(topic):
    consumer = KafkaConsumer(
        topic, bootstrap_servers=BOOTSTRAP_SERVERS, group_id=None,
        auto_offset_reset="earliest", enable_auto_commit=False, consumer_timeout_ms=3000,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    try:
        return [message.value for message in consumer]
    finally:
        consumer.close()


def epoch(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def iso(seconds):
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def csv_rule_matches(temperature, humidity, rule):
    def inside(value, low, high):
        low, high = float(low), float(high)
        return (low == high == -999) or low <= value <= high
    return inside(temperature, rule["temperature_min"], rule["temperature_max"]) and inside(
        humidity, rule["humidity_min"], rule["humidity_max"]
    )


def main():
    source = read_topic(SENSORS_TOPIC)
    alerts = read_topic(ALERTS_TOPIC)
    assert source and alerts, "Both input data and Kafka alerts must exist"
    counts = Counter(row["id"] for row in source)
    assert len(counts) >= 2
    with CONDITIONS_FILE.open() as file:
        rules = list(csv.DictReader(file))
    assert len(rules) == 4
    for row in source:
        assert 25 <= row["temperature"] <= 45 and 15 <= row["humidity"] <= 85
    max_time = max(row["timestamp"] for row in source)
    watermark = max_time - 10
    windows = {}
    for row in source:
        newest_start = math.floor(row["timestamp"] / 30) * 30
        for start in (newest_start - 30, newest_start):
            if start + 60 <= watermark:
                windows.setdefault(start, []).append(row)
    expected = {}
    for start, rows in windows.items():
        temperature = sum(r["temperature"] for r in rows) / len(rows)
        humidity = sum(r["humidity"] for r in rows) / len(rows)
        for rule in rules:
            if csv_rule_matches(temperature, humidity, rule):
                expected[(start, int(rule["code"]))] = (temperature, humidity, len(rows), rule)
    actual = Counter()
    for alert in alerts:
        start, end = epoch(alert["window"]["start"]), epoch(alert["window"]["end"])
        key = (start, alert["code"])
        assert key in expected, f"Unexpected alert: {alert}"
        temperature, humidity, count, rule = expected[key]
        assert end - start == 60 and start % 30 == 0
        assert end <= watermark
        assert math.isclose(alert["temperature_avg"], temperature, abs_tol=1e-9)
        assert math.isclose(alert["humidity_avg"], humidity, abs_tol=1e-9)
        assert alert["event_count"] == count
        assert alert["condition_id"] == int(rule["id"])
        assert alert["message"] == rule["message"]
        assert alert["timestamp"]
        actual[key] += 1
    assert actual == Counter({key: 1 for key in expected}), "Missing or duplicate finalized alerts"

    # Test all four CSV rules, inclusive boundaries, unused dimensions, and a normal case.
    spark = SparkSession.builder.master("local[2]").appName("VerifyCsvRules").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        conditions = spark.read.option("header", True).option("inferSchema", True).csv(str(CONDITIONS_FILE))
        averages = spark.createDataFrame([
            ("hotdry", 45.0, 15.0), ("coldwet", 25.0, 85.0),
            ("boundary", 30.0, 40.0), ("normal", 35.0, 50.0),
        ], ["case", "temperature_avg", "humidity_avg"])
        averages = averages.withColumn("window", F.struct(F.col("case").alias("start"), F.lit("end").alias("end"))).withColumn("event_count", F.lit(2))
        result = {(row.window.start, row.code) for row in select_alerts(averages, conditions).collect()}
        assert result == {("hotdry", 101), ("hotdry", 104), ("coldwet", 102),
                          ("coldwet", 103), ("boundary", 101), ("boundary", 103)}
    finally:
        spark.stop()
    print(json.dumps({"result": "PASS", "source_records": len(source),
                      "sensor_counts": dict(counts), "window_seconds": 60,
                      "slide_seconds": 30, "watermark_seconds": 10,
                      "finalized_windows": len(windows), "alerts": len(alerts),
                      "codes_in_random_stream": sorted({a["code"] for a in alerts}),
                      "all_means_match_input": True, "no_missing_or_duplicate_alerts": True,
                      "csv_rules_tested": [101, 102, 103, 104],
                      "csv_test_cases": 4, "last_event_time_utc": iso(max_time),
                      "watermark_utc": iso(watermark),
                      "note": "Only finalized windows are emitted; the final open windows await newer events."}, indent=2))


if __name__ == "__main__":
    main()
