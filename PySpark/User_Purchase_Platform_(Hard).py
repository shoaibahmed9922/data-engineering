from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

spending_df_data = [
    (1, "2019-07-01", "mobile", 100),
    (1, "2019-07-01", "desktop", 100),
    (2, "2019-07-01", "mobile", 100),
    (2, "2019-07-02", "mobile", 100),
    (3, "2019-07-01", "desktop", 100),
    (3, "2019-07-02", "desktop", 100),
]

spending_df_columns = ["user_id", "spend_date", "platform", "amount"]
spending_df = spark.createDataFrame(spending_df_data, spending_df_columns)
spending_df.show()

# 1. Generate a master template of all distinct dates combined with all 3 platform types
template_df = (
    spending_df.select("spend_date").distinct()
    .withColumn(
        "platform_type", 
        F.explode(F.array(F.lit("desktop"), F.lit("mobile"), F.lit("both")))
    )
)

template_df.show()

# Assuming your input DataFrame is loaded as 'spending_df'

result_df = (
    spending_df
    # 1. Pivot the platforms into separate columns
    .groupBy("user_id", "spend_date")
    .pivot("platform", ["desktop", "mobile"])
    .agg(F.sum("amount"))
    
    # 2. Assign platform type and calculate total amount per user per date
    .withColumn(
        "platform_type",
        F.when(F.col("desktop").isNotNull() & F.col("mobile").isNotNull(), "both")
         .when(F.col("desktop").isNotNull(), "desktop")
         .when(F.col("mobile").isNotNull(), "mobile")
    )
    .withColumn(
        "amount",
        F.coalesce(F.col("desktop"), F.lit(0)) + F.coalesce(F.col("mobile"), F.lit(0))
    )
    
    # 3. Final aggregation by date and platform type
    .groupBy("spend_date", "platform_type")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.countDistinct("user_id").alias("total_users")
    )
    
    # 4. Sort the output
    .orderBy("spend_date", "platform_type")
)

# 3. Left join the template with actual metrics and fill missing values with 0
result_df = (
    template_df.join(result_df, on=["spend_date", "platform_type"], how="left")
    .na.fill(0, subset=["total_amount", "total_users"])
    .orderBy("spend_date", "platform_type")
)

result_df.show()
