###################################    Question    ############################################
# The Question:
#  Given below is Total Grand Slam Titles Winner Data , 
#  -> Find the total number of Grand Slam Titles Won by each Player.
###############################################################################################

from pyspark.sql.functions import col, expr, lit
player_data = [
    (1, "Nedal"),
    (2, "Federer"),
    (3, "Novak")
]

matches_data = [
    (2017, 2, 1, 1, 2),
    (2018, 3, 1, 3, 2),
    (2019, 3, 1, 1, 3)
]
# create dataframes from data

player_df = spark.createDataFrame(player_data, ['player_id', 'player_name'])
matches_df = spark.createDataFrame(matches_data, ['year', 'Winbledon', 'Fr_open', 'Us_open', 'Au_open'])

################# Solution 1 #################

# use Spark's builtin function(stack) to unpivot pivoted data
unpivot_df_sol_1 = matches_df.select(expr("stack(4, 'Winbledon', Winbledon,'Fr_open', Fr_open, 'Us_open', Us_open, 'Au_open', Au_open) as (tounament, player_winner_id)"))

# aggregate and count wins by player
total_wins_df_sol_1 = unpivot_df_sol_1.groupBy('player_winner_id').count()

# add player name to player_id
result_df_sol_1 = total_wins_df_sol_1.join(player_df, player_df['player_id'] == total_wins_df_sol_1['player_winner_id']).select('player_id', 'player_name', 'count')

print('Solution 1')
result_df_sol_1.show()
result_df_sol_1.explain()

################# Solution 2 #################

# create separate dataframes for each column to union them later
unpivot_Winbledon = matches_df.select(col('year'), lit('Winbledon').alias('tounament'), col('Winbledon').alias('player_winner_id'))
unpivot_Fr_open = matches_df.select(col('year'), lit('Fr_open').alias('tounament'), col('Fr_open').alias('player_winner_id'))
unpivot_Us_open = matches_df.select(col('year'), lit('Us_open').alias('tounament'), col('Us_open').alias('player_winner_id'))
unpivot_Au_open = matches_df.select(col('year'), lit('Au_open').alias('tounament'), col('Au_open').alias('player_winner_id'))

# unionAll all the dataframes which contain relevant tournament winner player_id
unpivot_df_sol_2 = unpivot_Winbledon.unionAll(unpivot_Fr_open).unionAll(unpivot_Us_open).unionAll(unpivot_Au_open)

# aggregate and count wins by player
total_wins_df_sol_2 = unpivot_df_sol_2.groupBy('player_winner_id').count()

# add player name to player_id
result_df_sol_2 = total_wins_df_sol_2.join(player_df, player_df['player_id'] == total_wins_df_sol_2['player_winner_id']).select('player_id', 'player_name', 'count')

print('Solution 2')
result_df_sol_2.show()
result_df_sol_2.explain()

################# Spark SQL #################
matches_df.createOrReplaceTempView("matches_df")
player_df.createOrReplaceTempView("player_df")
query = """


WITH unpivot AS
(
    SELECT year, 'Winbledon' AS tounament, Winbledon AS player_winner_id FROM matches_df
    UNION ALL
    SELECT year, 'Fr_open' AS tounament, Fr_open AS player_winner_id FROM matches_df
    UNION ALL
    SELECT year, 'Us_open' AS tounament, Us_open AS player_winner_id FROM matches_df
    UNION ALL
    SELECT year, 'Au_open' AS tounament, Au_open AS player_winner_id FROM matches_df
),
total_wins AS
(
    SELECT player_winner_id, count(1) AS total_wins
    FROM unpivot
    GROUP BY 1
)
SELECT
    player_df.player_id,
    player_df.player_name,
    total_wins.total_wins
FROM total_wins
LEFT JOIN player_df ON player_df.player_id = total_wins.player_winner_id


"""
result_df_sol_3 = spark.sql(query)

print('SparkSQL solution')
result_df_sol_3.orderBy('player_id').show()
result_df_sol_3.explain()
