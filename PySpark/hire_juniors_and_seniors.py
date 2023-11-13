###################################    Question    ############################################
# Hire the candidates who fall under budget of 70000 according to below criteria:
# 📌 First hire Senior within budget
# 📌 Then hire Junior withing remaining budget.
###############################################################################################

from pyspark.sql.window import Window
from pyspark.sql.functions import col, when, sum

data = [
    (1, "Junior", 10000),
    (2, "Junior", 15000),
    (3, "Junior", 40000),
    (4, "Senior", 16000),
    (5, "Senior", 20000),
    (6, "Senior", 50000),
    (7, "Senior", 10000)
]

columns = ['emp_id', 'experience', 'salary']
df = spark.createDataFrame(data, columns)

seniors_commulative_salary = df.filter((col('experience') == 'Senior')).withColumn('commulative_salary', sum(col('salary')).over(Window.orderBy(col('salary'))))
juniors_commulative_salary = df.filter((col('experience') == 'Junior')).withColumn('commulative_salary', sum(col('salary')).over(Window.orderBy(col('salary'))))

senior_hires = seniors_commulative_salary.filter(col('commulative_salary') <= 70000)
total_budget_spent_on_seniors = senior_hires.agg(sum('salary')).collect()[0][0]

remaining_budget_for_juniors = 70000 - total_budget_spent_on_seniors
junior_hires = juniors_commulative_salary.filter(col('commulative_salary') <= remaining_budget_for_juniors)

all_hires = junior_hires.union(senior_hires)
all_hires = all_hires.withColumn('commulative_salary', sum(col('salary')).over(Window.orderBy(['salary', 'emp_id'])))
all_hires.show()
