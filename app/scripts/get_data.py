import nba_api.stats.endpoints as ep
import pandas as pd
import duckdb
import random
import time
import logging
import pickle
import os
import requests
from requests.exceptions import Timeout
from urllib3.exceptions import ReadTimeoutError
from multiprocessing import Pool
import sys
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


class DataFetcher:
    def __init__(self, season, logger, max_retries=3):
        self.season = season
        self.max_retries = max_retries
        self.FD = {
            'advanced': ep.boxscoreadvancedv2.BoxScoreAdvancedV2,
            'fourfactors': ep.boxscorefourfactorsv2.BoxScoreFourFactorsV2,
            'misc': ep.boxscoremiscv2.BoxScoreMiscV2,
            'scoring': ep.boxscorescoringv2.BoxScoreScoringV2,
            'summary': ep.boxscoresummaryv2.BoxScoreSummaryV2,
            'traditional': ep.boxscoretraditionalv2.BoxScoreTraditionalV2
        }
        self.gidset = set()  # will be set later
        self.log = self.fetch_log()
        self.logger=logger

    def fetch_log(self):
        try:
        # fetch games for a specific season (reason to separate 002 and 004 is becuase there are other games (non nba games) included in the GameFinder ep)
            result = ep.leaguegamefinder.LeagueGameFinder(season_nullable=self.season)
            all_games = result.get_data_frames()[0]
            rs = all_games[all_games.SEASON_ID == '2' + self.season[:4]]
            rs = rs[rs.GAME_ID.str[:3] == '002']  # regular season
            os = all_games[all_games.SEASON_ID == '4' + self.season[:4]]
            os = os[os.GAME_ID.str[:3] == '004']  # postseason
            nba_cup = all_games[all_games.SEASON_ID == '6' + self.season[:4]]
            nba_cup = nba_cup[nba_cup.GAME_ID.str[:3] == '006']  # nba cup
            log = pd.concat([rs, os,nba_cup])
            self.gidset.update(log['GAME_ID'].apply(lambda x: x.zfill(10)))  # Normalize game_id with leading zeros
        except Exception as e:
            self.logger.log_error(f"FAILED TO FETCH LOG FOR SEASON {self.season} {e}")
            return 0
        return log

    def fetch_game_data(self, endpoint_name, gid, writer=None):
        """
        Fetch game data for a single game, retrying with a delay on failure.
        If after max retries it still fails, skip the game and log the error.
        """
        statfunc = self.FD[endpoint_name]
        retries = 0

        while retries < self.max_retries:
            try:
                # Attempt to fetch the game data
                game = statfunc(game_id=gid).get_data_frames()
                players = game[0]
                teams = game[1]

                # If we successfully fetched the data, write it to CSV
                if writer:
                    writer.write_data(endpoint_name, teams, players, duckdb=True)

                return players, teams

            except (TimeoutError, Timeout, ReadTimeoutError) as e:
                retries += 1
                delay = random.uniform(1, 3) * (2 ** retries)  # Exponential backoff
                error_message = f"Timeout error for {gid} (Attempt {retries}/{self.max_retries}). Retrying in {delay:.2f}s..."
                # print(error_message)
                # self.logger.log_warning(error_message)  # Log warning with retry info
                time.sleep(delay)

            except Exception as e:
                # Any other errors are treated as permanent failure for this game
                error_message = f"Error fetching game data for {gid}: {e}. Skipping this game."
                # print(error_message)
                self.logger.log_error(error_message)  # Log the error
                return None, None  # Skip the game on failure

        # If retries exceeded, skip the game and log the failure
        error_message = f"Failed to fetch game data for {gid} after {self.max_retries} attempts. Skipping this game."
        # print(error_message)
        self.logger.log_error(error_message)  # Log the final failure
        return None, None  # Skip this game if max retries exhausted


class DataWriter:
    def __init__(self, season, logger):
        self.season = season
        self.data_path = 'app/data/raw'
        self.logger=logger

    def write_to_duckdb(self,endpoint_name,tstats,pstats):
        self.logger.log_info(f"attempting to insert {len(tstats) / 2} games into duckdb")
        try:
            ###
            conn = duckdb.connect(f'{DATABASE_PATH}/nba.db')

            team_table_exist = conn.sql(f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'raw' and TABLE_NAME = 'teams_{endpoint_name}'").fetchall()
            player_table_exist = conn.sql(f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'raw' and TABLE_NAME = 'players_{endpoint_name}'").fetchall()



            if team_table_exist[0][0]:
                insert_statement = f"""INSERT INTO raw.teams_{endpoint_name} SELECT * FROM tstats"""
                conn.execute(insert_statement)
                self.logger.log_sql(insert_statement)
            else:
                self.logger.log_warning(f"duckdb insert: no table found for: raw.teams_{endpoint_name}")
            if player_table_exist[0][0]:
                insert_statement = f"""INSERT INTO raw.players_{endpoint_name} SELECT * FROM pstats"""
                conn.execute(insert_statement)
                self.logger.log_sql(insert_statement)
            else:
                self.logger.log_warning(f"duckdb insert: no table found for raw.players_{endpoint_name}")

            conn.close()
        except Exception as e:
            self.logger.log_error(f"Attemping to insert into duckdb for {endpoint_name}, {e}")
            conn.close()

    def write_data(self, endpoint_name, tstats, pstats, duckdb=False):
        try:
            # Check if files already exist, and if they do, append the data
            team_file = f'{BASE_PATH}/{self.data_path}/teams/{endpoint_name}/{endpoint_name}{self.season}.csv'
            player_file = f'{BASE_PATH}/{self.data_path}/players/{endpoint_name}/{endpoint_name}{self.season}.csv'
            ## ensure location exists
            os.makedirs(os.path.dirname(team_file), exist_ok=True)
            os.makedirs(os.path.dirname(player_file), exist_ok=True)

            self.logger.log_info(f"attempting to write {len(tstats) / 2} games")

            # Append team if files exist, otherwise create new
            mode = 'a' if os.path.exists(team_file) else 'w'
            header = not os.path.exists(team_file)
            if mode == 'w':
                filtered_tstats = tstats
            else:
                current_tstats = pd.read_csv(team_file, usecols=['GAME_ID','TEAM_ID'], dtype={'GAME_ID': str,'TEAM_ID':int})
                filtered_tstats = tstats[~tstats[['GAME_ID', 'TEAM_ID']].apply(tuple, 1).isin(current_tstats[['GAME_ID', 'TEAM_ID']].apply(tuple, 1))]
            filtered_tstats.to_csv(team_file, mode=mode, header=header, index=False)

            # Append player if files exist, otherwise create new
            mode = 'a' if os.path.exists(player_file) else 'w'
            header = not os.path.exists(player_file)
            if mode == 'w':
                filtered_pstats = pstats
            else:
                current_pstats = pd.read_csv(player_file, usecols=['GAME_ID','TEAM_ID','PLAYER_ID'], dtype={'GAME_ID': str,'TEAM_ID':int,'PLAYER_ID':int})
                filtered_pstats = pstats[~pstats[['GAME_ID', 'TEAM_ID','PLAYER_ID']].apply(tuple, 1).isin(current_pstats[['GAME_ID', 'TEAM_ID','PLAYER_ID']].apply(tuple, 1))]
            filtered_pstats.to_csv(player_file, mode=mode, header=header, index=False)
            
            self.logger.log_info(f"Data written for {endpoint_name} - {self.season}")

            if duckdb:
                ### maybe input filtered stats?
                try:
                    self.write_to_duckdb(endpoint_name,tstats,pstats)
                except Exception as e:
                    self.logger.log_error(f"problem with  write_to_duckdb{endpoint_name} - {self.season}, ERROR: {e}")


        except Exception as e:
            self.logger.log_error(f"UNABLE TO WRITE DATA FOR {endpoint_name} - {self.season}, ERROR: {e}")


class DataChecker:
    def __init__(self, season, logger, data_path='app/data/raw'):
        self.season = season
        self.logger = logger
        self.data_path = data_path

    def get_processed_games(self, endpoint_name):
        """
        This method checks the existing files for a given season and endpoint,
        and returns the set of game IDs that have already been processed. 
        Only game IDs that are in both player and team stats files are considered.
        """
        player_games = set()
        team_games = set()

        # Check if the player stats file exists
        player_file = os.path.join(BASE_PATH, self.data_path, 'players', endpoint_name, f'{endpoint_name}{self.season}.csv')
        if os.path.exists(player_file):
            try:
                # Read the player stats CSV
                pstats = pd.read_csv(player_file, usecols=['GAME_ID'], dtype={'GAME_ID': str})
                # Normalize GAME_ID with leading zeros and add to player_games set
                player_games.update(pstats['GAME_ID'].apply(lambda x: x.zfill(10)))
            except Exception as e:
                self.logger.log_error(f"Error reading player stats file {endpoint_name}_{self.season}: {e}")

        # Check if the team stats file exists
        team_file = os.path.join(BASE_PATH, self.data_path, 'teams', endpoint_name, f'{endpoint_name}{self.season}.csv')
        if os.path.exists(team_file):
            try:
                # Read the team stats CSV
                tstats = pd.read_csv(team_file, usecols=['GAME_ID'], dtype={'GAME_ID': str})
                # Normalize GAME_ID with leading zeros and add to team_games set
                team_games.update(tstats['GAME_ID'].apply(lambda x: x.zfill(10)))
            except Exception as e:
                self.logger.log_error(f"Error reading team stats file {endpoint_name}_{self.season}: {e}")
        
        # Return the intersection of player_games and team_games
        return player_games & team_games  # Intersection of both sets

def update_log(season,logger,data_fetcher):
    data_path='app/data/raw'
    log_games=set()
    log_file = os.path.join(BASE_PATH, data_path, 'log', f'log{season}.csv')
    if os.path.exists(log_file):
        try:
            # Read the team stats CSV
            current_log = pd.read_csv(log_file, dtype={'GAME_ID': str})
            # Normalize GAME_ID with leading zeros and add to team_games set
            log_games.update(current_log['GAME_ID'].apply(lambda x: x.zfill(10)))
            

        except Exception as e:
            print(f"Error reading team stats file: {e}")
    else:
        current_log = data_fetcher.log
        logger.log_info(f"LOG_FILE for {season} IS OUT OF DATE, UPDATING")
        current_log.to_csv(log_file, index=False)
        return
        
    current_log = data_fetcher.log
    print(f"{len(log_games)}, {len(data_fetcher.gidset)}")
    missing_gids = data_fetcher.gidset - log_games

    if missing_gids:
        logger.log_info(f"LOG_FILE for {season} IS OUT OF DATE, UPDATING")
        current_log.to_csv(log_file, index=False)
    else:
        logger.log_info(f"log{season}.csv is up to date")    

def update_duckdb():
    """
    check files and update duckdb database with missing games
    """
    logger = Logger()
    logger.log_info(f"updating duckdb")
    try:
        ###
        conn = duckdb.connect(f'{DATABASE_PATH}/nba.db')

        for endpoint in ENDPOINTS:
            team_table_exist = conn.sql(f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'raw' and TABLE_NAME = 'teams_{endpoint}'").fetchall()
            player_table_exist = conn.sql(f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'raw' and TABLE_NAME = 'players_{endpoint}'").fetchall()
            if team_table_exist[0][0] and player_table_exist[0][0]:
                team_games = conn.sql(f"SELECT distinct GAME_ID FROM raw.teams_{endpoint}").df()
                player_games = conn.sql(f"SELECT distinct GAME_ID FROM raw.teams_{endpoint}").df()
                player_games.to_csv('sample_gam_id.csv')
                current_duckdb_games = set(team_games['GAME_ID']) & set(player_games['GAME_ID'])


            else:
                logger.log_error(f"NO {endpoint} table in duckdb {e}")
                break

            current_file_games = set()
            for season in SEASONS:
                checker = DataChecker(season,logger)
                current_file_games.update(checker.get_processed_games(endpoint))
            
            print(f"ENDPOINTS {endpoint} files= {len(current_file_games)}, duckdb={len(current_duckdb_games)}")
            # print(current_file_games - current_duckdb_games)


            ## UPDATE DUCKDB


        conn.close()
    except Exception as e:
        logger.log_error(f"updating duckdb, {e}")
        conn.close()

    return 0

def check_data():
    """
    checks most recent log, updates log file
    reads what is in files to see if they are behind the current log
    """

    logger = Logger()
    logger.log_info(f"\nCHECKING DATA")

    for season in SEASONS:
        # logger.log_info(f"Starting processing for season {season}")

        # Fetch log data
        data_fetcher = DataFetcher(season,logger)

        update_log(season,logger, data_fetcher)

        # Data checker for existing files
        checker = DataChecker(season,logger)

        # Process data for each endpoint
        for endpoint in ENDPOINTS:
            missing_gids = data_fetcher.gidset - checker.get_processed_games(endpoint)
            if missing_gids:
                logger.log_info(f"MISSING {len(missing_gids)} games for {endpoint}{season}.csv")

            else:
                logger.log_info(f"All data present for {endpoint}{season}.csv")

                
def main():
    # SEASONS = ['2023-24']


    # Initialize logger
    logger = Logger()
    logger.log_info(f"\nSTARTING NEW COLLECTION FOR SEASONS {SEASONS}, ENDPOINTS {ENDPOINTS}")

    for season in reversed(SEASONS):
        logger.log_info(f"Starting processing for season {season}")

        # Fetch log data
        data_fetcher = DataFetcher(season,logger)
        log = data_fetcher.fetch_log()

        # Data checker for existing files
        checker = DataChecker(season, logger)

        # Process data for each endpoint
        for endpoint in ENDPOINTS:

            # if int(season[:4]) < 1999 and endpoint != 'traditional': # CAN ONLY GET TRADITIONAL THIS FAR BACK
            #     continue

            missing_gids = data_fetcher.gidset - checker.get_processed_games(endpoint)
            empties = []
            
            if missing_gids:
                writer = DataWriter(season,logger)
                logger.log_info(f"Fetching {len(missing_gids)} missing games for {season}, {endpoint}")
                pstats_buffer = []
                tstats_buffer = []
                buffer_size = 100
                count = 0
                buffers=0
                for gid in missing_gids:
                    pstats, tstats = data_fetcher.fetch_game_data(endpoint, gid)

                    if pstats is not None and tstats is not None:
                        if pstats.empty or tstats.empty:
                            # logger.log_info(f"EMPTY GAME: {gid}")
                            empties.append(gid)
                            continue
                        pstats_buffer.append(pstats)
                        tstats_buffer.append(tstats)
                        count += 1
                        # logger.log_info(f"Fetched game {gid}")
                    # else:
                    #     logger.log_error(f"NO DATA FOR GAME_ID: {gid}")

                    if count > 0 and count % buffer_size == 0:
                        buffers+=1
                        # logger.log_info(f"Writing {count} games to disk...")
                        logger.log_info(f" {buffers*100}/{len(missing_gids)} game collection attempts")
                        writer.write_data(endpoint, pd.concat(tstats_buffer), pd.concat(pstats_buffer), duckdb=True)
                        ## TODO: also udate duckdb database
                        pstats_buffer = []
                        tstats_buffer = []
                        count = 0
                        
                if len(pstats_buffer):
                        # logger.log_info(f"Writing remaining {count} games to disk...")
                        logger.log_info(f" {count+buffers*100}/{len(missing_gids)} games collected, {len(tstats_buffer)} games")
                        writer.write_data(endpoint, pd.concat(tstats_buffer), pd.concat(pstats_buffer), duckdb=True)
                else:
                    logger.log_info(f"EMPTY DATAFRAMES FOR {endpoint}: {empties}")
                if len(empties):
                    logger.log_warning(f"{len(empties)} EMPTY GAMES")
                
                
            else:
                logger.log_info(f"All data already present for {endpoint}{season}.csv")


if __name__ == "__main__":
    # check_data()
    main()
    # update_duckdb()
