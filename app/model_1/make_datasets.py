import pandas as pd
import duckdb
import os


BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
DATABASE = os.path.join(BASE_PATH, 'app','database','nba.db')


print(BASE_PATH)
print(DATABASE)


### currently just four_factors
COLUMNS = ['EFG_PCT','FTA_RATE','TM_TOV_PCT','OREB_PCT','OPP_EFG_PCT','OPP_FTA_RATE','OPP_TOV_PCT','OPP_OREB_PCT']


def make_player_data(cols):
    """
    makes a query to get all player node data for usage in a gnn
    currently uses aggs.player_averages, could use aggs.player_averages_last_10
    maybe only include games after 5-10 have been played
    
    
    """


    player_query = """
    SELECT
        t2.GAME_DATE::VARCHAR || t1.NEXT_GAME_ID::VARCHAR || t1.PLAYER_ID::VARCHAR as NODE_ID
        ,t1.SEASON_ID
        ,t1.PLAYER_ID
        ,t2.TEAM_ID
        ,t1.NEXT_GAME_ID
        ,t2.PLAYED_FLAG"""

    for col in cols:
        player_query += f'\n\t,t1.AVG_{col}'
    
    player_query += """
    FROM aggs.player_averages t1
    join base.players_processed t2
    on t1.next_game_id = t2.game_id and t1.player_id = t2.player_id
    WHERE t2.SEASON_ID::VARCHAR not ilike '4%'"""
    return player_query

def main():
    player_query = make_player_data(COLUMNS)
    print(player_query)


if __name__ == "__main__":
    main()


