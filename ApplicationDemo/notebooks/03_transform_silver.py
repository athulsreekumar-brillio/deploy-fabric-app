# Fabric notebook source
# Attach Customer360Lakehouse as the default lakehouse before running.

from pyspark.sql import Window
from pyspark.sql import functions as F


run_id = "local-demo"

customers = spark.table("bronze_customers")
customer_window = Window.partitionBy("customer_id").orderBy(
    F.col("source_updated_at").desc(),
    F.col("_ingested_at").desc(),
)
deduplicated = (
    customers.withColumn("_row_number", F.row_number().over(customer_window))
    .filter(F.col("_row_number") == 1)
    .drop("_row_number")
)

valid_customer = (
    F.col("customer_id").isNotNull()
    & F.col("full_name").isNotNull()
    & F.col("email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
)
customer_rejects = deduplicated.filter(~valid_customer).withColumn(
    "_reject_reason",
    F.when(F.col("customer_id").isNull(), "missing_customer_id")
    .when(F.col("full_name").isNull(), "missing_full_name")
    .otherwise("invalid_email"),
)
silver_customers = (
    deduplicated.filter(valid_customer)
    .select("customer_id", "full_name", "email", "region", "source_updated_at")
    .withColumn("_run_id", F.lit(run_id))
)

transactions = spark.table("bronze_transactions")
valid_transaction = (
    F.col("transaction_id").isNotNull()
    & F.col("customer_id").isNotNull()
    & F.col("amount").cast("double").isNotNull()
    & (F.col("amount").cast("double") > 0)
    & F.col("event_timestamp").isNotNull()
)
known_customer = silver_customers.select("customer_id").distinct()
transactions_with_customer = transactions.join(known_customer, "customer_id", "left_semi")

transaction_rejects = transactions.filter(~valid_transaction).withColumn(
    "_reject_reason",
    F.when(F.col("transaction_id").isNull(), "missing_transaction_id")
    .when(F.col("amount").cast("double").isNull(), "invalid_amount")
    .when(F.col("amount").cast("double") <= 0, "non_positive_amount")
    .otherwise("missing_required_field"),
)
unknown_customer_rejects = (
    transactions.join(known_customer, "customer_id", "left_anti")
    .withColumn("_reject_reason", F.lit("unknown_or_rejected_customer"))
)
all_transaction_rejects = transaction_rejects.unionByName(
    unknown_customer_rejects, allowMissingColumns=True
)
silver_transactions = (
    transactions_with_customer.filter(valid_transaction)
    .select("transaction_id", "customer_id", F.col("amount").cast("double").alias("amount"),
            "currency", "event_timestamp")
    .withColumn("_run_id", F.lit(run_id))
)

silver_customers.write.format("delta").mode("overwrite").saveAsTable("silver_customers")
customer_rejects.write.format("delta").mode("overwrite").saveAsTable(
    "silver_customer_rejects"
)
silver_transactions.write.format("delta").mode("overwrite").saveAsTable(
    "silver_transactions"
)
all_transaction_rejects.write.format("delta").mode("overwrite").saveAsTable(
    "silver_transaction_rejects"
)

print(
    "Silver transform complete: {} customers, {} transactions, {} customer rejects, "
    "{} transaction rejects".format(
        silver_customers.count(),
        silver_transactions.count(),
        customer_rejects.count(),
        all_transaction_rejects.count(),
    )
)
