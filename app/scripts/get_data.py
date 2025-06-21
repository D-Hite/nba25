import nba_api.stats.endpoints as ep
import pandas as pd
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
        self.logger=logger

    def fetch_log(self):
        # fetch games for a specific season (reason to separate 002 and 004 is becuase there are other games (non nba games) included in the GameFinder ep)
        result = ep.leaguegamefinder.LeagueGameFinder(season_nullable=self.season)
        all_games = result.get_data_frames()[0]
        rs = all_games[all_games.SEASON_ID == '2' + self.season[:4]]
        rs = rs[rs.GAME_ID.str[:3] == '002']  # regular season
        os = all_games[all_games.SEASON_ID == '4' + self.season[:4]]
        os = os[os.GAME_ID.str[:3] == '004']  # postseason
        log = pd.concat([rs, os])
        self.gidset.update(log['GAME_ID'].apply(lambda x: x.zfill(10)))  # Normalize game_id with leading zeros
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
                    writer.write_data(endpoint_name, teams, players)

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

    def write_data(self, endpoint_name, tstats, pstats):
        try:
            # Sort the dataframes
            # tstats.sort_values('TEAM_ID', inplace=True, kind='mergesort')
            # tstats.sort_values('GAME_ID', inplace=True, kind='mergesort')
            # pstats.sort_values('TEAM_ID', inplace=True, kind='mergesort')
            # pstats.sort_values('GAME_ID', inplace=True, kind='mergesort')


            # Check if files already exist, and if they do, append the data
            team_file = f'{BASE_PATH}/{self.data_path}/teams/{endpoint_name}/{endpoint_name}{self.season}.csv'
            player_file = f'{BASE_PATH}/{self.data_path}/players/{endpoint_name}/{endpoint_name}{self.season}.csv'
            ## ensure location exists
            os.makedirs(os.path.dirname(team_file), exist_ok=True)

            self.logger.log_info(f"attempting to write {len(tstats) / 2} games")


            # Append if files exist, otherwise create new
            mode = 'a' if os.path.exists(team_file) else 'w'
            header = not os.path.exists(team_file)

            tstats.to_csv(team_file, mode=mode, header=header, index=False)
            pstats.to_csv(player_file, mode=mode, header=header, index=False)

            self.logger.log_info(f"Data written for {endpoint_name} - {self.season}")

        except Exception as e:
            print(f"Error while writing data for {endpoint_name}: {e}")


class DataChecker:
    def __init__(self, season, data_path='app/data/raw'):
        self.season = season
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
                pstats = pd.read_csv(player_file, dtype={'GAME_ID': str})
                # Normalize GAME_ID with leading zeros and add to player_games set
                player_games.update(pstats['GAME_ID'].apply(lambda x: x.zfill(10)))
            except Exception as e:
                print(f"Error reading player stats file: {e}")

        # Check if the team stats file exists
        team_file = os.path.join(BASE_PATH, self.data_path, 'teams', endpoint_name, f'{endpoint_name}{self.season}.csv')
        if os.path.exists(team_file):
            try:
                # Read the team stats CSV
                tstats = pd.read_csv(team_file, dtype={'GAME_ID': str})
                # Normalize GAME_ID with leading zeros and add to team_games set
                team_games.update(tstats['GAME_ID'].apply(lambda x: x.zfill(10)))
            except Exception as e:
                print(f"Error reading team stats file: {e}")
        
        # Return the intersection of player_games and team_games
        return player_games & team_games  # Intersection of both sets

def update_log(season,logger,log=pd.DataFrame()):
    data_path='app/data/raw'
    log_games=set()
    log_file = os.path.join(BASE_PATH, data_path, 'log', f'log{season}.csv')
    if os.path.exists(log_file):
        try:
            # Read the team stats CSV
            stats = pd.read_csv(log_file, dtype={'GAME_ID': str})
            # Normalize GAME_ID with leading zeros and add to team_games set
            log_games.update(stats['GAME_ID'].apply(lambda x: x.zfill(10)))
        except Exception as e:
            print(f"Error reading team stats file: {e}")
    else:
        if log.empty:
            log = data_fetcher.fetch_log()
        logger.log_info(f"LOG_FILE for {season} IS OUT OF DATE, UPDATING")
        log.to_csv(log_file, index=False)
        return
        
    data_fetcher = DataFetcher(season,logger)
    if log.empty:
        log = data_fetcher.fetch_log()
    missing_gids = data_fetcher.gidset - log_games

    if missing_gids:
        logger.log_info(f"LOG_FILE for {season} IS OUT OF DATE, UPDATING")
        log.to_csv(log_file, index=False)
    else:
        logger.log_info(f"log{season}.csv is up to date")    



def check_data():
    SEASONS = ['2010-11','2011-12','2012-13','2013-14','2014-15','2015-16','2016-17','2017-18','2018-19','2019-20'
               ,'2020-21','2021-22','2022-23','2023-24','2024-25']
    ENDPOINTS = ['advanced','fourfactors','misc','scoring','traditional']
    logger = Logger()
    logger.log_info(f"\nCHECKING DATA")

    for season in SEASONS:
        # logger.log_info(f"Starting processing for season {season}")

        # Fetch log data
        data_fetcher = DataFetcher(season,logger)
        log = data_fetcher.fetch_log()

        update_log(season,logger,log)

        # Data checker for existing files
        checker = DataChecker(season)

        # Process data for each endpoint
        for endpoint in ENDPOINTS:
            missing_gids = data_fetcher.gidset - checker.get_processed_games(endpoint)
            if missing_gids:
                logger.log_info(f"MISSING {len(missing_gids)} games for {endpoint}{season}.csv")

            else:
                logger.log_info(f"All data present for {endpoint}{season}.csv")

                
def main():
    SEASONS = ['2024-25']
    ENDPOINTS = ['traditional']

    # Initialize logger
    logger = Logger()
    logger.log_info(f"\nSTARTING NEW COLLECTION FOR SEASONS {SEASONS}, ENDPOINTS {ENDPOINTS}")

    for season in SEASONS:
        logger.log_info(f"Starting processing for season {season}")

        # Fetch log data
        data_fetcher = DataFetcher(season,logger)
        log = data_fetcher.fetch_log()

        # Data checker for existing files
        checker = DataChecker(season)

        # Process data for each endpoint
        for endpoint in ENDPOINTS:
            missing_gids = data_fetcher.gidset - checker.get_processed_games(endpoint)
            
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
                        writer.write_data(endpoint, pd.concat(tstats_buffer), pd.concat(pstats_buffer))
                        pstats_buffer = []
                        tstats_buffer = []
                        count = 0
                        
                if len(pstats_buffer):
                        # logger.log_info(f"Writing remaining {count} games to disk...")
                        logger.log_info(f" {count+buffers*100}/{len(missing_gids)} games collected")
                        writer.write_data(endpoint, pd.concat(tstats_buffer), pd.concat(pstats_buffer))
                

            else:
                logger.log_info(f"All data already present for {endpoint}{season}.csv")


if __name__ == "__main__":
    main()


    # check_data()

    # logger = Logger()
    # update_log('2024-25',logger)
