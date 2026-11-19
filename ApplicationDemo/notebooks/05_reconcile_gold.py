# Fabric notebook source
# Attach Customer360Lakehouse as the default lakehouse before running.

from datetime import datetime, timezone

from pyspark.sql import functions as F


run_id = "local-demo"
checked_at = datetime.now(timezone.utc).replace(tzinfo=None)

silver = spark.table("silver_transactions")
customer_gold = spark.table("gold_customer_360")
monthly_gold = spark.table("gold_monthly_revenue")

source_total = silver.agg(F.sum("amount").alias("total")).first()["total"] or 0.0
gold_total = (
    monthly_gold.agg(F.sum("revenue").alias("total")).first()["total"] or 0.0
)
duplicate_customer_keys = (
    customer_gold.groupBy("customer_id").count().filter(F.col("count") > 1).count()
)
missing_customer_keys = customer_gold.filter(F.col("customer_id").isNull()).count()
revenue_delta = abs(float(source_total) - float(gold_total))

checks = [
    ("customer_keys_unique", duplicate_customer_keys == 0, duplicate_customer_keys),
    ("customer_keys_not_null", missing_customer_keys == 0, missing_customer_keys),
    ("silver_to_gold_revenue", revenue_delta < 0.000001, revenue_delta),
]
failed_checks = [name for name, passed, _ in checks if not passed]

result_rows = [
    (run_id, name, "PASS" if passed else "FAIL", str(value), checked_at)
    for name, passed, value in checks
]
result_df = spark.createDataFrame(
    result_rows,
    ["run_id", "check_name", "status", "observed_value", "checked_at"],
)
result_df.write.format("delta").mode("overwrite").saveAsTable(
    "quality_reconciliation"
)

for name, passed, value in checks:
    print("{}: {} ({})".format(name, "PASS" if passed else "FAIL", value))

if failed_checks:
    raise ValueError("Gold reconciliation failed: {}".format(", ".join(failed_checks)))
