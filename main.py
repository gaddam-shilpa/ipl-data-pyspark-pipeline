from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, coalesce, col, when, sum, avg, row_number, date_format, regexp_replace, current_date, year
from pyspark.sql.window import Window
from src import schemas
from src import transformations as transf
from src import sql_queries as q

def main():
    # 1. Initialize Spark
    spark = SparkSession.builder.appName("IPL-Analytics").getOrCreate()
    base_path = "s3://ipl-data-analysis-project/"
    output_path = "/Volumes/workspace/default/data/IPL_Analytics/Output/"
    
    # 2. INGESTION: Read all raw CSVs using schemas from schemas.py
    print("*********************************************Reading datasets****************************************")
    ball_by_ball_df = spark.read.csv(f"{base_path}Ball_By_Ball.csv", header=True, schema=schemas.ball_by_ball_schema)
    match_df = spark.read.csv(f"{base_path}Match.csv", header=True, schema=schemas.match_schema)
    player_df = spark.read.csv(f"{base_path}Player.csv", header=True, schema=schemas.player_schema)
    player_match_df = spark.read.csv(f"{base_path}Player_match.csv", header=True, schema=schemas.player_match_schema)
    team_df = spark.read.csv(f"{base_path}Team.csv", header=True, schema=schemas.team_schema)
    print("*********************************************Reading datasets done****************************************")
    
    # 3. SILVER LAYER: Data Cleaning & Normalization
    # Clean Booleans
    print("*********************************************Cleaning & Normalizing datasets****************************************")
    boolean_cols_ball_df = ["caught","bowled","run_out","lbw","retired_hurt","stumped","caught_and_bowled","hit_wicket","obstructingfeild","bowler_wicket","keeper_catch"]
    ball_by_ball_df = transf.normalize_booleans(ball_by_ball_df, boolean_cols_ball_df)
    boolean_cols_player_match_df = ["isplayers_team_won", "is_manofthematch"]
    player_match_df = transf.normalize_booleans(player_match_df, boolean_cols_player_match_df)
    
    # Standardize Dates across multiple DataFrames
    ball_by_ball_df = transf.normalize_date(ball_by_ball_df,["match_date"])
    ball_by_ball_df = ball_by_ball_df.withColumn("matchdatesk", to_date("matchdatesk", "yyyyMMdd"))
    match_df = transf.normalize_date(match_df,["match_date"])
    player_df = transf.normalize_date(player_df,["dob"])
    player_match_df = transf.normalize_date(player_match_df,["dob"])
    print("*********************************************Cleaning & Normalizing datasets done****************************************")

    # 4. GOLD LAYER: Feature Engineering & Business Logic
    # Adding specialized columns
    print("*********************************************Feature Engineering & Business Logic ****************************************")
    ball_by_ball_df, total_and_average_runs = transf.transform_ball_df(ball_by_ball_df)
    match_df = transf.transform_match_df(match_df)
    player_df = transf.transform_palyer_df(player_df)
    player_match_df = transf.transform_player_match_df(player_match_df)
    print("*********************************************Feature Engineering & Business Logic done****************************************")

    # 5. ANALYTICS: Create Temp Views for Spark SQL
    print("***********************************************Creating views and running analytics*******************************************")
    ball_by_ball_df.createOrReplaceTempView("ball_by_ball")
    player_df.createOrReplaceTempView("player")
    player_match_df.createOrReplaceTempView("player_match")
    match_df.createOrReplaceTempView("matches")

    #SQL Queries
    top_scoring_batsmen_per_season = spark.sql(q.TOP_BATSMEN_PER_SEASON)
    economical_bowlers_powerplay = spark.sql(q.ECONOMICAL_BOWLERS)
    toss_impact_individual_matches = spark.sql(q.TOSS_IMPACT)
    average_runs_in_wins = spark.sql(q.AVERAGE_RUNS)
    scores_by_venue = spark.sql(q.SCORES)
    print("***********************************************Creating views and running analytics done*******************************************")

    # 6. Storing the results
    print("***********************************************Storing the results*******************************************")
    output = {
        "ball_by_ball_transformed_data":ball_by_ball_df,
        "player_df_transformed_data":player_df,
        "player_match_df_transformed_data":player_match_df,
        "match_df_transformed_data":match_df,
        "top_scoring_batsmen_per_season_sql_query_output":top_scoring_batsmen_per_season,
        "economical_bowlers_powerplay_sql_query_output":economical_bowlers_powerplay,
        "toss_impact_individual_matches_sql_query_output":toss_impact_individual_matches,
        "average_runs_in_wins_sql_query_output":average_runs_in_wins,
        "scores_by_venue_sql_query_output":scores_by_venue
    }

    for folder_name, df in output.items():
        df.coalesce(1).write.format("csv")\
            .mode("overwrite")\
            .option("header","true")\
            .save(f"{output_path}{folder_name}")


if __name__ == "__main__":
    main()
