# Question | https://leetcode.com/problems/trips-and-users/description/

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, BooleanType, TimestampType, DateType
from datetime import datetime
from pyspark.sql.functions import col, count, when, round
trips_data = [
    (1, 10 ,1 ,'completed',           datetime.strptime('2013-10-01', "%Y-%m-%d")),
    (2, 11 ,1 ,'cancelled_by_driver', datetime.strptime('2013-10-01', "%Y-%m-%d")),
    (3, 12 ,6 ,'completed',           datetime.strptime('2013-10-01', "%Y-%m-%d")),
    (4, 13 ,6 ,'cancelled_by_client', datetime.strptime('2013-10-01', "%Y-%m-%d")),
    (1, 10 ,1 ,'completed',           datetime.strptime('2013-10-02', "%Y-%m-%d")),
    (2, 11 ,6 ,'completed',           datetime.strptime('2013-10-02', "%Y-%m-%d")),
    (3, 12 ,6 ,'completed',           datetime.strptime('2013-10-02', "%Y-%m-%d")),
    (2, 12 ,12 ,'completed',          datetime.strptime('2013-10-03', "%Y-%m-%d")),
    (3, 10 ,12 ,'completed',          datetime.strptime('2013-10-03', "%Y-%m-%d")),
    (4, 13 ,12 ,'cancelled_by_driver',datetime.strptime('2013-10-03', "%Y-%m-%d")),
]
trips_schema = StructType([
    StructField('client_id', IntegerType(), False),
    StructField('driver_id', IntegerType(), False),
    StructField('city_id', IntegerType(), False),
    StructField('status', StringType(), False),
    StructField('request_at', DateType(), False)
])
trips_df = spark.createDataFrame(trips_data, schema = trips_schema)
# trips_df.show()

users_data = [
    (1, 'No', 'client'),
    (2, 'Yes', 'client'),
    (3, 'No', 'client'),
    (4, 'No', 'client'),
    (10, 'No', 'driver'),
    (11, 'No', 'driver'),
    (12, 'No', 'driver'),
    (13, 'No', 'driver'),
]
users_schema = StructType([
    StructField('users_id', IntegerType(), False),
    StructField('banned', StringType(), False),
    StructField('role', StringType(), False)
])
users_df = spark.createDataFrame(users_data, schema = users_schema)
client_status_df = trips_df.join(users_df, trips_df['client_id'] == users_df['users_id'], how = 'left').filter(col('banned') == 'No').select(trips_df["*"])
driver_status_df = client_status_df.join(users_df, client_status_df['driver_id'] == users_df['users_id'], how = 'left').filter(col('banned') == 'No').select(client_status_df["*"])
res_df = driver_status_df.groupBy('request_at').agg(
                                                      count(when(driver_status_df.status.like('cancelled%'), 1)).alias("cancelled_cnt"),
                                                      count(col('client_id')).alias("total_cnt")
                                                   )
res_df.select('request_at', round((res_df.cancelled_cnt/res_df.total_cnt), 2).alias("cancellation_rate")).orderBy('request_at', ascending = [1]).show()
