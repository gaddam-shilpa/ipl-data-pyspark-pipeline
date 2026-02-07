from pyspark.sql.functions import to_date, coalesce, col, when, sum, avg, row_number, date_format, regexp_replace, current_date, year, month, dayofmonth 
from pyspark.sql.window import Window

#Converting the boolean columns
def normalize_booleans(df, boolean_cols):
    """Converts 1/0 or strings to proper BooleanTypes."""
    for column in boolean_cols:
        df = df.withColumn(column,
            when(col(column)==1, True)
            .when(col(column)==0,False)
            .otherwise(None)
        )
    return df

#Normalizing date format
def normalize_date(df,date_columns):
    """Handles hyphen to slash replacement and date casting."""
    for c in date_columns:
        df = df.withColumn(c, regexp_replace(col(c), "-", "/"))
        df = df.withColumn(c, to_date(col(c), "M/d/yyyy"))
    return df

#Tranformations
def transform_ball_df(ball_by_ball_df):
    #1. Filter to include only valid deliveries (excluding extras like wides and no balls for specific analysis)"""
    ball_by_ball_df = ball_by_ball_df.filter((col("wides") == 0) & (col("noballs") == 0))

    #We define this before aggregation because it relies on the individual rows
    #2. Window funct: Calculate running total of runs in each match  for each over"""
    WindowSpec = Window.partitionBy("match_id", "over_id").orderBy("ball_id")
    ball_by_ball_df = ball_by_ball_df.withColumn(
        "running_total_runs",
        sum("runs_scored").over(WindowSpec)
    )

    #3. Condition formatting: Flag for high impact balls  (either a wicket or more than 6 runs including extras)"""
    ball_by_ball_df = ball_by_ball_df.withColumn(
        "high_impact",
        when(((col("runs_scored") + col("extra_runs")) > 6) | (col("bowler_wicket") == True), True).otherwise(False)
    )

    #4. Aggregation: Calculate the total and average runs scored in each match and inning"""
    total_and_average_runs = ball_by_ball_df.groupBy("match_id", "innings_no").agg(
        sum("runs_scored").alias("total_runs"),
        avg("runs_scored").alias("avg_runs")
    )
    return ball_by_ball_df, total_and_average_runs


def transform_match_df(match_df):
    #1. Extracting year, month, and day from match_date for more detailed time-based analysis"""
    match_df = match_df.withColumn("year", year("match_date"))
    match_df = match_df.withColumn("month", month("match_date"))
    match_df = match_df.withColumn("day", dayofmonth("match_date"))

    #2. High margins win: Categorizing high, low, medium"""
    match_df = match_df.withColumn("win_margin_category",
                                when((col("win_margin") >= 100), "High")
                                .when(((col("win_margin") >= 50) & (col("win_margin") < 100)),"Medium")
                                .otherwise("Low"))

    #3. Analyse the impact of toss: who wins the toss and who won"""
    match_df = match_df.withColumn("toss_match_winner",
                                when((col("toss_winner") == col("match_winner")), "Yes")
                                .otherwise("No"))
    return match_df

def transform_palyer_df(player_df):
    #1. Normalize and clean player name
    player_df = player_df.withColumn("player_name", regexp_replace(col("player_name"), "[^a-zA-Z0-9 ]", ""))

    #2. Handle missing values in batting_hand and bowling_skill with a default_name
    player_df = player_df.na.fill({"batting_hand":"unknown", "bowling_skill":"unknown"})

    #3. Categorizing players based on batting hand
    player_df = player_df.withColumn("batting_style",
                                    when(col("batting_hand").contains("Left"), "Left-Handed")
                                    .otherwise("Right-Handed"))
    return player_df


def transform_player_match_df(player_match_df):
    #Add a 'veteran_status' column based on player age
    player_match_df = player_match_df.withColumn("veteran_status", 
                                    when((col("age_as_on_match") >= 35), "Veteran")
                                    .otherwise("Non-Veteran")
                                    )
    #Filter to include who played the match (excluding bench players)
    #Dynamic column to calculate years since debut
    player_match_df = player_match_df.withColumn("years_since_debut", year(current_date()) - col("season_year"))
    return player_match_df