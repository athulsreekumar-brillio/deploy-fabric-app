# Fabric notebook source
# Attach Customer360Lakehouse as the default lakehouse before running.

from pyspark.sql import functions as F


run_id = "local-demo"

customers = spark.table("silver_customers").alias("c")
transactions = spark.table("silver_transactions").alias("t")

customer_totals = transactions.groupBy("customer_id").agg(
    F.countDistinct("transaction_id").alias("transaction_count"),
    F.sum("amount").alias("lifetime_revenue"),
    F.max("event_timestamp").alias("last_transaction_at"),
)

customer_360 = (
    customers.join(customer_totals, "customer_id", "left")
    .fillna({"transaction_count": 0, "lifetime_revenue": 0.0})
    .withColumn("_run_id", F.lit(run_id))
)

monthly_revenue = (
    transactions.withColumn("revenue_month", F.date_format("event_timestamp", "yyyy-MM"))
    .groupBy("revenue_month", "currency")
    .agg(
        F.countDistinct("transaction_id").alias("transaction_count"),
        F.sum("amount").alias("revenue"),
    )
    .withColumn("_run_id", F.lit(run_id))
)

customer_360.write.format("delta").mode("overwrite").saveAsTable("gold_customer_360")
monthly_revenue.write.format("delta").mode("overwrite").saveAsTable(
    "gold_monthly_revenue"
)

print(
    "Gold tables built: {} customer summaries and {} monthly aggregates".format(
        customer_360.count(), monthly_revenue.count()
    )
)
