"""
this files purpose is to 
    read all the distinct rows in the raw files
    read all the distinct rows present in duckdb
    update duckdb if there is data in the raw files no present in the duckdb database
"""
import os
import duckdb
import glob
from ..utils.logger import Logger



BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
# Your data path inside the 'collection' directory
DATA_PATH = os.path.join(BASE_PATH,  'app','data', 'raw')
DATABASE_PATH = os.path.join(BASE_PATH, 'app','database')



SEASONS = [
            '1990-91','1991-92','1992-93','1993-94','1994-95',
            '1995-96','1996-97','1997-98','1998-99','1999-00',
            '2000-01','2001-02','2002-03','2003-04','2004-05',
            '2005-06','2006-07','2007-08','2008-09','2009-10',
            '2010-11','2011-12','2012-13','2013-14','2014-15',
            '2015-16','2016-17','2017-18','2018-19','2019-20',
            '2020-21','2021-22','2022-23','2023-24','2024-25'
        ]
ENDPOINTS = ['advanced','fourfactors','misc','scoring','traditional']


PRIMARY_KEYS = {
    'players': ('GAME_ID', 'PLAYER_ID'),
    'teams': ('GAME_ID', 'TEAM_ID'),
    'log': ('GAME_ID', 'TEAM_ID'),
    'lines': ('GAME_ID', 'TEAM_ABBREVIATION')
}

def update_duckdb():
    """
    Check files and update DuckDB database with missing games.
    """
    logger = Logger()
    logger.log_info("Updating DuckDB")
    conn = None
    try:
        conn = duckdb.connect(f"{DATABASE_PATH}/nba.db")

        for endpoint in ENDPOINTS:
            team_table_exist = conn.sql(f"""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = 'raw' 
                  AND TABLE_NAME = 'teams_{endpoint}'
            """).fetchone()[0]

            player_table_exist = conn.sql(f"""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = 'raw' 
                  AND TABLE_NAME = 'players_{endpoint}'
            """).fetchone()[0]

            if not (team_table_exist and player_table_exist):
                logger.log_error(f"No {endpoint} table in DuckDB")
                continue

            team_path = f"{DATA_PATH}/teams/{endpoint}/"
            player_path = f"{DATA_PATH}/players/{endpoint}/"

            team_csv_files = glob.glob(f"{team_path}*.csv")
            player_csv_files = glob.glob(f"{player_path}*.csv")

            team_file_paths_str = ", ".join([f"'{file}'" for file in team_csv_files])
            player_file_paths_str = ", ".join([f"'{file}'" for file in player_csv_files])

            # --- Teams ---
            team_missing_count = conn.sql(f"""
                SELECT COUNT(*) AS cnt
                FROM (
                    SELECT n.GAME_ID, n.TEAM_ID
                    FROM read_csv_auto([{team_file_paths_str}], union_by_name=true, files_to_sniff=-1, nullstr='') n
                    LEFT JOIN raw.teams_{endpoint} t
                      ON n.GAME_ID = t.GAME_ID
                     AND n.TEAM_ID = t.TEAM_ID
                    WHERE t.GAME_ID IS NULL
                )
            """).fetchone()[0]

            if team_missing_count > 0:
                cur = conn.execute(f"""
                    INSERT INTO raw.teams_{endpoint}
                    SELECT n.*
                    FROM read_csv_auto([{team_file_paths_str}], union_by_name=true, files_to_sniff=-1, nullstr='') n
                    LEFT JOIN raw.teams_{endpoint} t
                      ON n.GAME_ID = t.GAME_ID
                     AND n.TEAM_ID = t.TEAM_ID
                    WHERE t.GAME_ID IS NULL
                """)
                logger.log_info(f"{endpoint}: inserted {team_missing_count} new team rows")

            # --- Players ---
            player_missing_count = conn.sql(f"""
                SELECT COUNT(*) AS cnt
                FROM (
                    SELECT n.GAME_ID, n.PLAYER_ID
                    FROM read_csv_auto([{player_file_paths_str}], union_by_name=true, files_to_sniff=-1, nullstr='') n
                    LEFT JOIN raw.players_{endpoint} t
                      ON n.GAME_ID = t.GAME_ID
                     AND n.PLAYER_ID = t.PLAYER_ID
                    WHERE t.GAME_ID IS NULL
                    AND n.PLAYER_ID IS NOT NULL
                )
            """).fetchone()[0]

            if player_missing_count > 0:
                cur = conn.execute(f"""
                    INSERT INTO raw.players_{endpoint}
                    SELECT n.*
                    FROM read_csv_auto([{player_file_paths_str}], union_by_name=true, files_to_sniff=-1, nullstr='') n
                    LEFT JOIN raw.players_{endpoint} t
                      ON n.GAME_ID = t.GAME_ID
                     AND n.PLAYER_ID = t.PLAYER_ID
                    WHERE t.GAME_ID IS NULL
                    AND n.PLAYER_ID IS NOT NULL
                """)
                logger.log_info(f"{endpoint}: inserted {player_missing_count} new player rows")

        conn.close()
    except Exception as e:
        logger.log_error(f"Updating DuckDB failed: {e}")
        if conn:
            conn.close()

    return 0


def test_update_duckdb():
    """
    Check files and update DuckDB database with missing games.
    """
    logger = Logger()
    logger.log_info("Updating DuckDB")
    conn = None
    try:
        conn = duckdb.connect(f"{DATABASE_PATH}/nba.db")

        for endpoint in ENDPOINTS:
            team_table_exist = conn.sql(f"""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = 'raw' 
                  AND TABLE_NAME = 'teams_{endpoint}'
            """).fetchone()[0]

            player_table_exist = conn.sql(f"""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = 'raw' 
                  AND TABLE_NAME = 'players_{endpoint}'
            """).fetchone()[0]

            if not (team_table_exist and player_table_exist):
                logger.log_error(f"No {endpoint} table in DuckDB")
                continue

            team_path = f"{DATA_PATH}/teams/{endpoint}/"
            player_path = f"{DATA_PATH}/players/{endpoint}/"

            team_csv_files = glob.glob(f"{team_path}*.csv")
            player_csv_files = glob.glob(f"{player_path}*.csv")

            team_file_paths_str = ", ".join([f"'{file}'" for file in team_csv_files])
            player_file_paths_str = ", ".join([f"'{file}'" for file in player_csv_files])

            # --- Teams ---
            team_missing_count = conn.sql(f"""
SELECT COUNT(*) AS missing_team_rows
FROM read_csv_auto([{team_file_paths_str}], union_by_name=true, files_to_sniff=-1, nullstr='') n
LEFT JOIN raw.teams_{endpoint} t
  ON n.GAME_ID::INT = t.GAME_ID
 AND n.TEAM_ID::INT = t.TEAM_ID
WHERE t.GAME_ID IS NULL;

            """).fetchone()[0]

            if team_missing_count > 0:
                logger.log_info(f"{endpoint}: {team_missing_count} missing team rows")

            # --- Players ---
            player_missing_count = conn.sql(f"""
SELECT COUNT(*) AS missing_player_rows
FROM read_csv_auto([{player_file_paths_str}], union_by_name=true, files_to_sniff=-1, nullstr='') n
LEFT JOIN raw.players_{endpoint} t
  ON n.GAME_ID::INT = t.GAME_ID
 AND n.PLAYER_ID::INT = t.PLAYER_ID
WHERE t.GAME_ID IS NULL;

            """).fetchone()[0]

            if player_missing_count > 0:
                logger.log_info(f"{endpoint}: {player_missing_count} missing player rows")

            rows = conn.execute(f"""SELECT n.*
                    FROM read_csv_auto([{player_file_paths_str}], union_by_name=true, files_to_sniff=-1, nullstr='') n
                    LEFT JOIN raw.players_{endpoint} t
                      ON n.GAME_ID = t.GAME_ID
                     AND n.PLAYER_ID = t.PLAYER_ID
                    WHERE t.GAME_ID IS NULL""").df()
            # rows.to_csv(f'sample_player_missing_{endpoint}.csv')

        conn.close()
    except Exception as e:
        logger.log_error(f"Updating DuckDB failed: {e}")
        if conn:
            conn.close()

    return 0




if __name__ == "__main__":
    # test_update_duckdb()
    update_duckdb()
