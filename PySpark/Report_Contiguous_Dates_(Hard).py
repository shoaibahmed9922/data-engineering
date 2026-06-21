from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta

failed_data_1225 = [
    ("2018-12-28",), ("2018-12-29",), ("2019-01-04",), ("2019-01-05",)
]
failed_df_1225 = spark.createDataFrame(failed_data_1225, ["date"]).select(col('date').cast("date").alias('failed_date'))
failed_df_1225.show()

succeeded_data_1225 = [
    ("2018-12-30",), ("2018-12-31",), ("2019-01-01",), 
    ("2019-01-02",), ("2019-01-03",), ("2019-01-06",)
]

succeeded_df_1225 = spark.createDataFrame(succeeded_data_1225, ["date"]).select(col('date').cast("date").alias('success_date'))
succeeded_df_1225.show()()

start_date = '2019-01-01'
start_date = datetime.strptime(start_date, '%Y-%m-%d')
end_date = '2019-12-31'
end_date = datetime.strptime(end_date, '%Y-%m-%d')
calender = [(start_date+timedelta(days=i),) for i in range((end_date - start_date).days+1)]
calender_df = spark.createDataFrame(calender, ["dt"])
show(calender_df.limit(10))

df = calender_df.join(failed_df_1225, failed_df_1225.failed_date == calender_df.dt, "left")\
.join(succeeded_df_1225, succeeded_df_1225.success_date == calender_df.dt, "left")
df = df.withColumn('period_state', when(col('failed_date').isNotNull(), 'failed').otherwise(when(col('success_date').isNotNull(), 'succeeded').otherwise(None)))\
    .withColumn('prev_state', lag("period_state").over(Window.orderBy(col('dt').asc())))

df = df.withColumn('is_new_group', when((col('prev_state').isNull()) | (col('prev_state') != coalesce(col('period_state'), lit(''))), lit(1)).otherwise(lit(0)))

df = df.filter(col('period_state').isNotNull())

df = df.withColumn('group_id',sum('is_new_group').over(Window.orderBy(col('dt').asc())))
df = df.groupBy('group_id', 'period_state').agg(
    min(col('dt')).alias('start_date'),
    max(col('dt')).alias('end_date')
)
df.select("period_state", "start_date", "end_date").show()