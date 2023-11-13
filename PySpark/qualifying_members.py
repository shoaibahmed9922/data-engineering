###################################    Question    ############################################
# Find out which player qualifies according to following rules:
# 1. for a member criteria 1 and criteria 2 should be Y
# 2. there must be at least 2 members of that team that qualifies then only a team qualifies
###############################################################################################

from pyspark.sql.window import Window
from pyspark.sql.functions import col, when, count, coalesce, lit

# create dataframe from data
data = [
    ("T1", "T1_mbr1", "Y", "Y"),
    ("T1", "T1_mbr2", "Y", "Y"),
    ("T1", "T1_mbr3", "Y", "Y"),
    ("T1", "T1_mbr4", "Y", "Y"),
    ("T1", "T1_mbr5", "Y", "N"),
    ("T2", "T2_mbr1", "Y", "Y"),
    ("T2", "T2_mbr2", "Y", "N"),
    ("T2", "T2_mbr3", "N", "Y"),
    ("T2", "T2_mbr4", "N", "N"),
    ("T2", "T2_mbr5", "N", "N"),
    ("T3", "T3_mbr1", "Y", "Y"),
    ("T3", "T3_mbr2", "Y", "Y"),
    ("T3", "T3_mbr3", "N", "Y"),
    ("T3", "T3_mbr4", "N", "Y"),
    ("T3", "T3_mbr5", "Y", "N")
]
df = spark.createDataFrame(data, ['teamID', 'memberID', 'Criteria1', 'Criteria2'])

# create a column to demonstrate 1st criteria
df = df.withColumn('Qualified', when((col('Criteria1') == 'Y') & (col('Criteria2') == 'Y'), "Y").otherwise("N"))

# get all the teams who meet both 1st & 2nd criteria
df_teams_qualified = df.filter(col('Qualified') == 'Y').select('teamID').groupBy('teamID').count().filter(col('count') >= 2).select(col('teamID').alias('QualifiedteamID'))

# Now get members who belong to qualified teams in dataframe 'df_teams_qualified'
df = df.join(df_teams_qualified, df['teamID'] == df_teams_qualified['QualifiedteamID'], 'left')

# check if team is in QualifiedteamID dataframe and 1st criteri met then mark the member as qualified
df = df.withColumn('Qualified', when((col('QualifiedteamID').isNull() == False) & (col('Qualified') == 'Y'), 'Y').otherwise('N'))

# select necessary columns
df = df.select('teamID', 'memberID', 'Criteria1', 'Criteria2', 'Qualified')
df.show()
