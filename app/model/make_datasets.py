import pandas as pd
import duckdb
import os

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
DATABASE = os.path.join(BASE_PATH,'app','database','nba.db')
COLUMNS = ['EFG_PCT','FTA_RATE','TM_TOV_PCT','OREB_PCT','OPP_EFG_PCT','OPP_FTA_RATE','OPP_TOV_PCT','OPP_OREB_PCT']

def make_valid_games_cte(season_exclude='4%', season_min=21996, game_count_min=1):
    """
    Returns a SQL snippet for a CTE that defines valid games based on your criteria.
    """
    return f"""
WITH team_game_counts AS (
    SELECT GAME_ID,
           TEAM_ID,
           GAME_NUMBER
    FROM base.teams_processed
    WHERE SEASON_ID::VARCHAR NOT ILIKE '{season_exclude}' AND SEASON_ID > {season_min}
),
valid_games AS (
    SELECT g.GAME_ID
    FROM team_game_counts g
    JOIN team_game_counts h
      ON g.GAME_ID = h.GAME_ID
     AND g.TEAM_ID != h.TEAM_ID
    WHERE g.GAME_NUMBER >= {game_count_min} AND h.GAME_NUMBER >= {game_count_min}
)
    """

def make_player_nodes(conn, valid_games_cte, cols=COLUMNS, output_dir=None):
    col_select = ",\n\t".join([f"t1.AVG_{c}" for c in cols])
    query = f"""
{valid_games_cte}
SELECT
    t1.NEXT_GAME_ID::VARCHAR || '_' || t1.PLAYER_ID::VARCHAR AS node_id,
    t1.SEASON_ID,
    t1.PLAYER_ID,
    t2.TEAM_ID,
    t1.NEXT_GAME_ID,
    t2.PLAYED_FLAG,
    {col_select}
FROM aggs.player_averages t1
JOIN base.players_processed t2
  ON t1.NEXT_GAME_ID = t2.GAME_ID
 AND t1.PLAYER_ID = t2.PLAYER_ID
JOIN valid_games vg
  ON t1.NEXT_GAME_ID = vg.GAME_ID
WHERE t1.NEXT_GAME_ID IS NOT NULL
    """
    df = conn.execute(query).df()
    if output_dir:
        df.to_csv(os.path.join(output_dir, 'player_nodes.csv'), index=False)
    return df

def make_team_nodes(conn, valid_games_cte, output_dir=None):
    query = f"""
{valid_games_cte}
SELECT DISTINCT
    t.NEXT_GAME_ID::VARCHAR || '_' || t.TEAM_ID::VARCHAR AS node_id,
    t.TEAM_ID,
    t.SEASON_ID,
    t.NEXT_GAME_ID,
    t.TEAM_CITY,
    t.IS_HOME,
    t.DAYS_SINCE_LAST_GAME,
    t.IS_BACK_TO_BACK,
    t.IS_3_IN_4,
    t.IS_4_IN_6,
    t.WINS_SO_FAR,
    t.LOSSES_SO_FAR,
    t.LAST_10_WIN_PCT,
    t.WINS_VS_OPPONENT,
    t.LOSSES_VS_OPPONENT,
    t.IS_LAST_TEAM_GAME
FROM base.teams_processed t
JOIN valid_games vg
  ON t.NEXT_GAME_ID = vg.GAME_ID
    """
    df = conn.execute(query).df()
    if output_dir:
        df.to_csv(os.path.join(output_dir, 'team_nodes.csv'), index=False)
    return df

def make_outcome_nodes(conn, valid_games_cte, output_dir=None):
    query = f"""
{valid_games_cte}
SELECT
    o.GAME_ID::VARCHAR AS node_id,
    CASE WHEN o.WL = 'W' then 1 else 0 end AS result
FROM base.teams_processed o
JOIN valid_games vg
  ON o.GAME_ID = vg.GAME_ID
WHERE o.IS_HOME = TRUE
GROUP BY o.GAME_ID, o.WL
    """
    df = conn.execute(query).df()
    if output_dir:
        df.to_csv(os.path.join(output_dir, 'outcome_nodes.csv'), index=False)
    return df

def create_edges(conn, valid_games_cte, output_dir=None):
    # Player -> Team
    player_team_query = f"""
{valid_games_cte}
SELECT
    p.NEXT_GAME_ID::VARCHAR || '_' || p.PLAYER_ID::VARCHAR AS source_node_id,
    t.NEXT_GAME_ID::VARCHAR || '_' || t.TEAM_ID::VARCHAR AS target_node_id,
    'plays_for' AS edge_type
FROM aggs.player_averages p
JOIN base.teams_processed t
  ON p.NEXT_GAME_ID = t.NEXT_GAME_ID
 AND p.TEAM_ID = t.TEAM_ID
JOIN valid_games vg
  ON p.NEXT_GAME_ID = vg.GAME_ID
    """
    df_pt = conn.execute(player_team_query).df()
    if output_dir:
        df_pt.to_csv(os.path.join(output_dir, 'player_team_edges.csv'), index=False)

    # Team -> Outcome
    team_outcome_query = f"""
{valid_games_cte}
SELECT
    t.NEXT_GAME_ID::VARCHAR || '_' || t.TEAM_ID::VARCHAR AS source_node_id,
    o.GAME_ID::VARCHAR AS target_node_id,
    'team_result' AS edge_type
FROM base.teams_processed t
JOIN base.teams_processed o
  ON t.NEXT_GAME_ID = o.GAME_ID
 AND o.IS_HOME = TRUE
JOIN valid_games vg
  ON t.NEXT_GAME_ID = vg.GAME_ID
    """
    df_to = conn.execute(team_outcome_query).df()
    if output_dir:
        df_to.to_csv(os.path.join(output_dir, 'team_outcome_edges.csv'), index=False)

    return df_pt, df_to

# Example main
def main():
    conn = duckdb.connect(DATABASE)
    output_dir = os.path.join(BASE_PATH, 'app','model','data')

    valid_games_cte = make_valid_games_cte(season_exclude='4%', season_min=21996, game_count_min=8)

    player_nodes = make_player_nodes(conn, valid_games_cte, output_dir=output_dir)
    team_nodes = make_team_nodes(conn, valid_games_cte, output_dir=output_dir)
    outcome_nodes = make_outcome_nodes(conn, valid_games_cte, output_dir=output_dir)
    create_edges(conn, valid_games_cte, output_dir=output_dir)

if __name__ == "__main__":
    main()
