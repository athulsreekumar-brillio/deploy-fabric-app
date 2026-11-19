# Fabric notebook source
# Attach Customer360Lakehouse as the default lakehouse before running.

from datetime import datetime, timezone

from pyspark.sql import functions as F


run_id = "local-demo"
ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)

customers = [
    (1001, "Avery Chen", "avery.chen@example.test", "WEST", "2026-10-12T09:00:00"),
    (1002, "Jordan Patel", "jordan.patel@example.test", "EAST", "2026-10-13T10:30:00"),
    (1003, "Sam Rivera", "invalid-email", "CENTRAL", "2026-10-14T08:15:00"),
    (1002, "Jordan Patel", "jordan.patel@example.test", "EAST", "2026-10-15T11:45:00"),
    (1004, "Taylor Kim", None, "WEST", "2026-10-16T12:00:00"),
]

source_df = spark.createDataFrame(
    customers,
    ["customer_id", "full_name", "email", "region", "source_updated_at"],
)

bronze_df = (
    source_df.withColumn("source_updated_at", F.to_timestamp("source_updated_at"))
    .withColumn("_run_id", F.lit(run_id))
    .withColumn("_ingested_at", F.lit(ingested_at).cast("timestamp"))
)

bronze_df.write.format("delta").mode("overwrite").saveAsTable("bronze_customers")
print("Wrote {} synthetic customer rows for run {}".format(bronze_df.count(), run_id))
