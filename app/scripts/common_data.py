import os
import glob
import duckdb
import pandas as pd
from nba_api.stats.endpoints import commonplayerinfo
from ..utils.logger import Logger  # Assuming relative import in your structure

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
DATA_PATH = os.path.join(BASE_PATH, 'app', 'data', 'raw')
DATABASE_PATH = os.path.join(BASE_PATH, 'app', 'database')

class CommonPlayerInfoCollector:
    def __init__(self, logger):
        self.logger = logger
        self.output_path = os.path.join(DATA_PATH, 'players', 'common')
        os.makedirs(self.output_path, exist_ok=True)

    def get_all_player_ids(self):
        """
        Collects all distinct player IDs from the existing player game data (e.g., traditional box scores).
        """
        traditional_path = os.path.join(DATA_PATH, 'players', 'traditional')
        csv_files = glob.glob(f'{traditional_path}/*.csv')

        if not csv_files:
            self.logger.log_error("No traditional player CSV files found.")
            return pd.DataFrame()

        file_paths_str = ', '.join([f"'{f}'" for f in csv_files])
        query = f"""
            SELECT DISTINCT player_id 
            FROM read_csv_auto([{file_paths_str}], union_by_name=true, files_to_sniff=-1, nullstr='')
        """
        df = duckdb.sql(query).df()
        df = df.dropna(subset=['PLAYER_ID']).astype({'PLAYER_ID': 'int'})
        self.logger.log_info(f"Found {len(df)} unique player IDs.")
        return df

    def fetch_player_info(self, player_id):
        """
        Fetches player metadata from the NBA API.
        """
        try:
            response = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
            df = response.get_data_frames()[0]
            return df
        except Exception as e:
            self.logger.log_error(f"Failed to fetch player info for {player_id}: {e}")
            return pd.DataFrame()

    def write_to_csv(self, df):
        """
        Writes the combined player info to a CSV file.
        """
        file_path = os.path.join(self.output_path, 'common_player_info.csv')
        df.to_csv(file_path, index=False)
        self.logger.log_info(f"Wrote player info for {len(df)} players to {file_path}")

    ## TODO
    # def write_to_duckdb(self, df):
    #     """
    #     Optionally inserts the player metadata into a DuckDB table.
    #     """
    #     try:
    #         conn = duckdb.connect(os.path.join(DATABASE_PATH, 'nba.db'))
    #         conn.sql("CREATE SCHEMA IF NOT EXISTS raw")
    #         col_defs = ', '.join([f'"{col}" TEXT' for col in df.columns])
    #         conn.sql(f"""
    #             CREATE TABLE IF NOT EXISTS raw.common_player_info ({col_defs})
    #         """)
    #         conn.register("temp_player_info", df)
    #         conn.sql("INSERT INTO raw.common_player_info SELECT * FROM temp_player_info")
    #         conn.close()
    #         self.logger.log_info(f"Inserted {len(df)} rows into DuckDB.")
    #     except Exception as e:
    #         self.logger.log_error(f"Failed to insert into DuckDB: {e}")

def main():
    logger = Logger()
    collector = CommonPlayerInfoCollector(logger)

    player_ids_df = collector.get_all_player_ids()
    if player_ids_df.empty:
        logger.log_warning("No player IDs found. Exiting.")
        return

    all_info = []
    for pid in player_ids_df['PLAYER_ID']:
        df = collector.fetch_player_info(pid)
        if not df.empty:
            all_info.append(df)

    if all_info:
        final_df = pd.concat(all_info, ignore_index=True)
        collector.write_to_csv(final_df)
        # collector.write_to_duckdb(final_df)
    else:
        logger.log_warning("No player info data was successfully fetched.")

if __name__ == "__main__":
    main()
