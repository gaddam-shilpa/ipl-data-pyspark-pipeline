#SQL Queries
# 1. Season-wise Batting Leaderboard
# Logic: Joins ball-by-ball with match and player metadata to aggregate total runs.
# We group by player and season to identify the top performers for each IPL year.
TOP_BATSMEN_PER_SEASON = """
SELECT  
    p.player_name,
    m.season_year,
    SUM(b.runs_scored) AS total_runs
FROM ball_by_ball b
JOIN matches m ON b.match_id = m.match_id
JOIN player_match pm ON m.match_id = pm.match_id AND b.striker = pm.player_id
JOIN player p ON p.player_id = pm.player_id
GROUP BY p.player_name, m.season_year
ORDER BY m.season_year, total_runs DESC
"""


# 2. Powerplay Bowling Efficiency (Over 1-6)
# Logic: Specifically filters for the first 6 overs to find specialists. 
# Uses HAVING clause to ensure statistical significance (minimum 120 balls/20 overs bowled).
# Ranks by lowest economy (avg_runs_per_ball) and highest wicket count.
ECONOMICAL_BOWLERS = ("""
SELECT p.player_name,
    AVG(b.runs_scored) AS avg_runs_per_ball,
    COUNT(b.bowler_wicket) AS total_wickets
FROM ball_by_ball b
JOIN player_match pm ON b.match_id = pm.match_id AND b.bowler = pm.player_id
JOIN player p ON pm.player_id = p.player_id
WHERE b.over_id <= 6
GROUP BY p.player_name
HAVING COUNT(*) > 120
ORDER BY avg_runs_per_ball, total_wickets DESC
""")


# 3. Toss Impact Correlation Analysis
# Logic: Compares the toss_winner with the match_winner using a CASE statement.
# This identifies if winning the toss provides a significant statistical advantage.
TOSS_IMPACT = ("""
SELECT
    m.match_id,
    m.toss_winner,
    m.toss_name,
    m.match_winner,
    CASE WHEN m.toss_winner = m.match_winner THEN 'Won' ELSE 'Lost' END AS match_outcome
FROM matches m
WHERE m.toss_name IS NOT NULL
ORDER BY m.match_id
""")


# 4. Match-Winning Batting Contributions
# Logic: Joins match data to filter specifically for innings where the striker's team won.
# Helps identify "Match Winners"—players who perform consistently well under winning pressure.
AVERAGE_RUNS = ("""
SELECT p.player_name,
    AVG(b.runs_scored) AS avg_runs_in_wins,
    COUNT(*) AS innings_played  
FROM ball_by_ball b
JOIN player_match pm ON b.match_id = pm.match_id AND b.striker = pm.player_id
JOIN player p ON pm.player_id = p.player_id
JOIN matches m ON pm.match_id = m.match_id
WHERE m.match_winner = pm.player_team
GROUP BY p.player_name
ORDER BY avg_runs_in_wins DESC
""")

# 5. Venue-based Scoring Trends (Subquery Pattern)
# Logic: First calculates total runs per match (Inner Query), 
# then calculates average and peak scores across different stadiums (Outer Query).
# Useful for identifying "High Scoring Grounds" vs "Bowler Friendly Tracks".
SCORES = ("""
SELECT venue_name, AVG(total_runs) AS average_score, MAX(total_runs) AS highest_score
FROM (
    SELECT b.match_id, m.venue_name, SUM(b.runs_scored) AS total_runs
    FROM ball_by_ball b
    JOIN matches m ON b.match_id = m.match_id
    GROUP BY b.match_id, m.venue_name
)
GROUP BY venue_name
ORDER BY average_score DESC
""")