from datetime import datetime, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# --- Data Preparation ---

# Users DataFrame
users_data = [
    (1, "2019-01-01", "Lenovo"),
    (2, "2019-02-09", "Samsung"),
    (3, "2019-01-19", "LG"),
    (4, "2019-05-21", "HP"),
]
users_columns = ["user_id", "join_date", "favorite_brand"]
users_df = spark.createDataFrame(users_data, users_columns)
users_df.show()

# Orders DataFrame
orders_data = [
    (1, "2019-08-01", 4, 1, 2),
    (2, "2019-08-02", 2, 1, 3),
    (3, "2019-08-03", 3, 2, 3),
    (4, "2019-08-04", 1, 4, 2),
    (5, "2019-08-04", 1, 3, 4),
    (6, "2019-08-05", 2, 2, 4),
]
orders_columns = ["order_id", "order_date", "item_id", "buyer_id", "seller_id"]
orders_df = spark.createDataFrame(orders_data, orders_columns)
orders_df.show()

# Items DataFrame
items_data = [
    (1, "Samsung"),
    (2, "Lenovo"),
    (3, "LG"),
    (4, "HP"),
]
items_columns = ["item_id", "item_brand"]
items_df = spark.createDataFrame(items_data, items_columns)
items_df.show()


# --- Transformations ---

# Get the second sold item for each seller
window_spec = Window.partitionBy("seller_id").orderBy(asc("order_date"))

orders_df1 = (
    orders_df.join(users_df, orders_df.seller_id == users_df.user_id, "left")
    .join(items_df, items_df.item_id == orders_df.item_id, "left")
    .withColumn("rank", row_number().over(window_spec))
    .filter(col("rank") == 2)
    .select("seller_id", col("item_brand").alias("second_item_brand"))
)
orders_df1.display()

# Check if the second sold item matches the seller's favorite brand
final_df = (
    users_df.join(orders_df1, orders_df1.seller_id == users_df.user_id, "left")
    .withColumn(
        "2nd_item_fav_brand",
        when(col("favorite_brand") == col("second_item_brand"), lit("Yes")).otherwise(lit("No")),
    )
    .select("user_id", "2nd_item_fav_brand")
)
final_df.display()
