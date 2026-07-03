# ==============================================================
# Imports
# ==============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()


# ==============================================================
# Configuration
# ==============================================================

DIM_TABLE = "gold.customer_scd2"

columns = [
    "customer_id",
    "customer_name",
    "customer_city",
    "customer_state"
]

# Columns whose changes should create a new SCD version
tracked_cols = [
    "customer_name",
    "customer_city",
    "customer_state"
]


# ==============================================================
# STEP 1 - Create Initial Customer Data
# ==============================================================

customers = [
    ("C001", "John", "Lahore", "Punjab"),
    ("C002", "Ali", "Karachi", "Sindh"),
    ("C003", "Sara", "Islamabad", "ICT")
]

silver_df = spark.createDataFrame(customers, columns)


# ==============================================================
# STEP 2 - Prepare Initial Dimension Records
#
# Add:
#   • Change detection hash
#   • Surrogate key
#   • SCD metadata
# ==============================================================

silver_df = (
    silver_df
        .withColumn(
            "record_hash",
            sha2(concat_ws("||", *tracked_cols), 256)
        )
        .withColumn("customer_sk", expr("uuid()"))
        .withColumn("effective_start_date", current_timestamp())
        .withColumn(
            "effective_end_date",
            to_timestamp(lit("9999-12-31 23:59:59"))
        )
        .withColumn("is_current", lit(True))
        .withColumn("created_at", current_timestamp())
        .withColumn("updated_at", current_timestamp())
)


# ==============================================================
# STEP 3 - Create Initial SCD Type 2 Dimension
# ==============================================================

(
    silver_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(DIM_TABLE)
)


# ==============================================================
# STEP 4 - Simulate Day-2 Source Data
#
# C001 -> Changed
# C004 -> New customer
# ==============================================================

customers_day2 = [
    ("C001", "John", "Karachi", "Sindh"),
    ("C002", "Ali", "Karachi", "Sindh"),
    ("C003", "Sara", "Islamabad", "ICT"),
    ("C004", "Ahmed", "Lahore", "Punjab")
]

updates = spark.createDataFrame(customers_day2, columns)

updates = (
    updates
        .withColumn(
            "record_hash",
            sha2(concat_ws("||", *tracked_cols), 256)
        )
)


# ==============================================================
# STEP 5 - Load Current Dimension Records
# ==============================================================

dim = DeltaTable.forName(spark, DIM_TABLE)

current = (
    dim.toDF()
       .filter(col("is_current"))
)


# ==============================================================
# STEP 6 - Detect Changed Customers
#
# Join source with current dimension and compare hashes.
# Only customers with attribute changes are returned.
# ==============================================================

changed = (
    updates.alias("src")
        .join(
            current.alias("tgt"),
            "customer_id"
        )
        .filter(
            col("src.record_hash") !=
            col("tgt.record_hash")
        )
        .select("src.*")
)


# ==============================================================
# STEP 7 - Materialize Changed Records
#
# Serverless compute does not support persist()/cache()
# reliably, so materialize the dataframe instead.
# ==============================================================

TEMP_TABLE = "gold.temp_customer_scd2"

(
    changed
        .write
        .mode("overwrite")
        .saveAsTable(TEMP_TABLE)
)


# ==============================================================
# STEP 8 - Expire Existing Versions
#
# Current records become historical records.
# ==============================================================

(
    dim.alias("tgt")
       .merge(
           spark.table(TEMP_TABLE).alias("src"),
           """
           tgt.customer_id = src.customer_id
           AND tgt.is_current = true
           """
       )
       .whenMatchedUpdate(
           set={
               "is_current": "false",
               "effective_end_date": "current_timestamp()",
               "updated_at": "current_timestamp()"
           }
       )
       .execute()
)


# ==============================================================
# STEP 9 - Insert New Versions
#
# Insert a fresh current record for every changed customer.
# ==============================================================

changed = spark.table(TEMP_TABLE)

new_versions = (
    changed
        .withColumn("customer_sk", expr("uuid()"))
        .withColumn("effective_start_date", current_timestamp())
        .withColumn(
            "effective_end_date",
            to_timestamp(lit("9999-12-31 23:59:59"))
        )
        .withColumn("is_current", lit(True))
        .withColumn("created_at", current_timestamp())
        .withColumn("updated_at", current_timestamp())
)

(
    new_versions
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(DIM_TABLE)
)


# ==============================================================
# STEP 10 - Detect Brand-New Customers
#
# Left Anti Join returns customers that do not exist in
# the current dimension.
# ==============================================================

new_inserts = (
    updates.join(
        current,
        "customer_id",
        "leftanti"
    )
)


# ==============================================================
# STEP 11 - Add SCD Metadata to New Customers
# ==============================================================

new_inserts = (
    new_inserts
        .withColumn("customer_sk", expr("uuid()"))
        .withColumn("effective_start_date", current_timestamp())
        .withColumn(
            "effective_end_date",
            to_timestamp(lit("9999-12-31 23:59:59"))
        )
        .withColumn("is_current", lit(True))
        .withColumn("created_at", current_timestamp())
        .withColumn("updated_at", current_timestamp())
)


# ==============================================================
# STEP 12 - Insert New Customers
# ==============================================================

(
    new_inserts
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(DIM_TABLE)
)
