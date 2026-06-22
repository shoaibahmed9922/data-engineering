from datetime import datetime, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# --- Data Preparation ---
activity_data = [
    (1, 2, "2016-03-01", 5),
    (1, 2, "2016-03-02", 6),
    (2, 3, "2017-06-25", 1),
    (3, 1, "2016-03-01", 0),
    (3, 4, "2016-07-03", 5),
]
activity_columns_1097 = ["player_id", "device_id", "event_date", "games_played"]

activity_df = spark.createDataFrame(activity_data, activity_columns_1097)
activity_df.display()


# --- Transformations ---

# Define the window specification for player installations
player_window = Window.partitionBy("player_id")

# Calculate install date and flag next-day retention logins
activity_df = (
    activity_df.withColumn("install_date", min("event_date").over(player_window))
    .withColumn(
        "logged_on_next_day",
        when(date_add(col("event_date"), -1) == col("install_date"), lit(1)).otherwise(lit(0))
    )
)

# Calculate total installs and day-1 retention rate per install date
final_metrics_df = (
    activity_df.groupBy("install_date")
    .agg(
        countDistinct("player_id").alias("installs"),
        (
            countDistinct(when(col("logged_on_next_day") == 1, col("player_id"))) 
            / countDistinct("player_id")
        ).alias("day1_retention")
    )
)

final_metrics_df.show()
