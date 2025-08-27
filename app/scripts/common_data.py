import os
import glob
import duckdb
import pandas as pd
from nba_api.stats.endpoints import commonplayerinfo
from ..utils.logger import Logger  # Adjust relative import as needed

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
DATA_PATH = os.path.join(BASE_PATH, 'app', 'data', 'raw')
DATABASE_PATH = os.path.join(BASE_PATH, 'app', 'database')

class CommonPlayerInfoCollector:
    def __init__(self, logger):
        self.logger = logger
        self.output_path = os.path.join(DATA_PATH, 'players', 'common')
        os.makedirs(self.output_path, exist_ok=True)
        self.output_file = os.path.join(self.output_path, 'common_player_info.csv')

    def get_all_player_ids(self):
        traditional_path = os.path.join(DATA_PATH, 'players', 'traditional')
        csv_files = glob.glob(f'{traditional_path}/*.csv')

        if not csv_files:
            self.logger.log_error("No traditional player CSV files found.")
            return pd.DataFrame()

        file_paths_str = ', '.join([f"'{f}'" for f in csv_files])
        query = f"""
            SELECT DISTINCT player_id 
            FROM read_csv_auto([{file_paths_str}], union_by_name=true, files_to_sniff=-1, nullstr='')
            WHERE PLAYER_NAME is not NULL;
        """
        df = duckdb.sql(query).df()
        df = df.dropna(subset=['PLAYER_ID']).astype({'PLAYER_ID': 'int'})
        self.logger.log_info(f"Found {len(df)} unique player IDs.")
        return df

    def get_existing_player_ids(self):
        if not os.path.exists(self.output_file):
            return set()

        try:
            df = pd.read_csv(self.output_file, usecols=['PERSON_ID'])
            existing_ids = set(df['PERSON_ID'].dropna().astype(int))
            self.logger.log_info(f"Found {len(existing_ids)} existing players in file.")
            return existing_ids
        except Exception as e:
            self.logger.log_warning(f"Could not read existing player file. Rebuilding from scratch: {e}")
            return set()

    def fetch_player_info(self, player_id):
        try:
            response = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
            df = response.get_data_frames()[0]
            return df
        except Exception as e:
            self.logger.log_error(f"Failed to fetch player info for {player_id}: {e}")
            return pd.DataFrame()

    def append_to_csv(self, df):
        mode = 'a' if os.path.exists(self.output_file) else 'w'
        header = not os.path.exists(self.output_file)
        df.to_csv(self.output_file, mode=mode, header=header, index=False)
        self.logger.log_info(f"Appended {len(df)} new players to {self.output_file}")

    # def write_to_duckdb(self, df):
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
    #         self.logger.log_info(f"Inserted {len(df)} new rows into DuckDB.")
    #     except Exception as e:
    #         self.logger.log_error(f"Failed to insert into DuckDB: {e}")

def main():
    logger = Logger()
    collector = CommonPlayerInfoCollector(logger)

    all_ids_df = collector.get_all_player_ids()
    if all_ids_df.empty:
        logger.log_warning("No player IDs found. Exiting.")
        return

    all_ids = set(all_ids_df['PLAYER_ID'])
    existing_ids = collector.get_existing_player_ids()
    missing_ids = all_ids - existing_ids

    logger.log_info(f"{len(missing_ids)} new players to update.")

    if not missing_ids:
        logger.log_info("common_player_info.csv is already up to date.")
        return

    all_new_data = []
    for pid in sorted(missing_ids):
        df = collector.fetch_player_info(pid)
        if not df.empty:
            all_new_data.append(df)

    if all_new_data:
        result_df = pd.concat(all_new_data, ignore_index=True)
        collector.append_to_csv(result_df)
        # collector.write_to_duckdb(result_df)
    else:
        logger.log_warning("No new player info was fetched.")

if __name__ == "__main__":
    main()
