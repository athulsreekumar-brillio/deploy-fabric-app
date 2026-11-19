# Fabric notebook source
# Attach Customer360Lakehouse as the default lakehouse before running.

from datetime import datetime, timezone

from pyspark.sql import functions as F


run_id = "local-demo"
ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)

transactions = [
    ("T-9001", 1001, 120.50, "USD", "2026-10-20T14:05:00"),
    ("T-9002", 1001, 48.00, "USD", "2026-10-21T09:10:00"),
    ("T-9003", 1002, 240.00, "USD", "2026-10-21T16:20:00"),
    ("T-9004", 1003, 75.25, "USD", "2026-10-22T11:00:00"),
    ("T-9005", 1004, -10.00, "USD", "2026-10-23T12:00:00"),
    ("T-9006", 9999, 15.00, "USD", "2026-10-24T08:30:00"),
]

source_df = spark.createDataFrame(
    transactions,
    ["transaction_id", "customer_id", "amount", "currency", "event_timestamp"],
)

bronze_df = (
    source_df.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
    .withColumn("_run_id", F.lit(run_id))
    .withColumn("_ingested_at", F.lit(ingested_at).cast("timestamp"))
)

bronze_df.write.format("delta").mode("overwrite").saveAsTable("bronze_transactions")
print("Wrote {} synthetic transaction rows for run {}".format(bronze_df.count(), run_id))
