# Importing requried libraries
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

#Ball by Ball data
ball_by_ball_schema = StructType([

    StructField("match_id", IntegerType(), True),
    StructField("over_id", IntegerType(), True),
    StructField("ball_id", IntegerType(), True),
    StructField("innings_no", IntegerType(), True),

    StructField("team_batting", StringType(), True),
    StructField("team_bowling", StringType(), True),

    StructField("striker_batting_position", IntegerType(), True),

    StructField("extra_type", StringType(), True),
    StructField("runs_scored", IntegerType(), True),
    StructField("extra_runs", IntegerType(), True),

    StructField("wides", IntegerType(), True),
    StructField("legbyes", IntegerType(), True),
    StructField("byes", IntegerType(), True),
    StructField("noballs", IntegerType(), True),
    StructField("penalty", IntegerType(), True),
    StructField("bowler_extras", IntegerType(), True),

    StructField("out_type", StringType(), True),

    StructField("caught", StringType(), True),
    StructField("bowled", StringType(), True),
    StructField("run_out", StringType(), True),
    StructField("lbw", StringType(), True),
    StructField("retired_hurt", StringType(), True),
    StructField("stumped", StringType(), True),
    StructField("caught_and_bowled", StringType(), True),
    StructField("hit_wicket", StringType(), True),
    StructField("obstructingfeild", StringType(), True),
    StructField("bowler_wicket", StringType(), True),

    StructField("match_date", StringType(), True),
    StructField("season", IntegerType(), True),

    StructField("striker", IntegerType(), True),
    StructField("non_striker", IntegerType(), True),
    StructField("bowler", IntegerType(), True),
    StructField("player_out", IntegerType(), True),
    StructField("fielders", IntegerType(), True),

    StructField("striker_match_sk", IntegerType(), True),
    StructField("strikersk", IntegerType(), True),
    StructField("nonstriker_match_sk", IntegerType(), True),
    StructField("nonstriker_sk", IntegerType(), True),
    StructField("fielder_match_sk", IntegerType(), True),
    StructField("fielder_sk", IntegerType(), True),
    StructField("bowler_match_sk", IntegerType(), True),
    StructField("bowler_sk", IntegerType(), True),
    StructField("playerout_match_sk", IntegerType(), True),

    StructField("battingteam_sk", IntegerType(), True),
    StructField("bowlingteam_sk", IntegerType(), True),

    StructField("keeper_catch", StringType(), True),
    StructField("player_out_sk", IntegerType(), True),

    StructField("matchdatesk", StringType(), True)
])


# Match csv data
match_schema = StructType([

    StructField("match_sk", IntegerType(), True),
    StructField("match_id", IntegerType(), True),

    StructField("team1", StringType(), True),
    StructField("team2", StringType(), True),

    StructField("match_date", StringType(), True),
    StructField("season_year", IntegerType(), True),

    StructField("venue_name", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("country_name", StringType(), True),

    StructField("toss_winner", StringType(), True),
    StructField("match_winner", StringType(), True),
    StructField("toss_name", StringType(), True),

    StructField("win_type", StringType(), True),
    StructField("outcome_type", StringType(), True),

    StructField("manofmach", StringType(), True),
    StructField("win_margin", IntegerType(), True),

    StructField("country_id", IntegerType(), True)
])


# Player Data
player_schema = StructType([
    StructField("player_sk",IntegerType(),True),
    StructField("player_id",IntegerType(),True),
    StructField("player_name",StringType(),True),
    StructField("dob",StringType(),True),
    StructField("batting_hand",StringType(),True),
    StructField("bowling_skill",StringType(),True),
    StructField("country_name",StringType(),True)
])


#Team Data
team_schema = StructType([
    StructField("team_sk", IntegerType(), True),
    StructField("team_id", IntegerType(), True),
    StructField("team_name", StringType(), True)
])


#Player Match Data
player_match_schema = StructType([

    StructField("player_match_sk", IntegerType(), True),

    # decimal needs precision & scale chosen safely
    # StructField("playermatch_key", DecimalType(18, 0), True),
    StructField("playermatch_key", StringType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("player_id", IntegerType(), True),

    StructField("player_name", StringType(), True),
    StructField("dob", StringType(), True),

    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("role_desc", StringType(), True),

    StructField("player_team", StringType(), True),
    StructField("opposit_team", StringType(), True),

    # Spark has no YearType use IntegerType
    StructField("season_year", IntegerType(), True),

    StructField("is_manofthematch", StringType(), True),
    StructField("age_as_on_match", IntegerType(), True),
    StructField("isplayers_team_won", StringType(), True),

    StructField("batting_status", StringType(), True),
    StructField("bowling_status", StringType(), True),

    StructField("player_captain", StringType(), True),
    StructField("opposit_captain", StringType(), True),

    StructField("player_keeper", StringType(), True),
    StructField("opposit_keeper", StringType(), True)
])