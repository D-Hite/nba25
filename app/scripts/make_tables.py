import pandas as pd
import glob
import time
import duckdb
import os

from ..utils.logger import Logger


# Define the base path relative to the project root
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# Your data path inside the 'collection' directory
DATA_PATH = os.path.join(BASE_PATH,  'app','data', 'raw')
DATABASE_PATH = os.path.join(BASE_PATH, 'app','database')


# SQLMESH_PATH = os.path.join(BASE_PATH, 'app', 'sqlmesh')
# print(f"Data Path: {SQLMESH_PATH}")


# Just a check to confirm paths
print(f"Base Path: {BASE_PATH}")
print(f"Data Path: {DATA_PATH}")
print(f"Data Path: {DATABASE_PATH}")



class TableGenerator():

    def __init__(self, logger):
        self.conn = duckdb.connect(f'{DATABASE_PATH}/nba.db')
        self.CURRENT_TABLES =  self.get_endpoints()
        self.logger = logger


    def get_endpoints(self):
        team_table_paths = glob.glob(f'{DATA_PATH}/teams/*')
        team_tables = set([x.split('/')[-1] for x in team_table_paths])

        player_table_paths = glob.glob(f'{DATA_PATH}/players/*')
        player_tables = set([x.split('/')[-1] for x in player_table_paths])

        return team_tables.intersection(player_tables)

    def schema_exists(self, schema_name):
        """Check if a schema exists using information_schema."""
        result = self.conn.execute(f"""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name = '{schema_name}'
        """).fetchall()
        return len(result) > 0
    
    def create_log_table(self):
        log_data_path = f"{DATA_PATH}/log/"
        log_csv_files = glob.glob(f'{log_data_path}*.csv')

        if not log_csv_files:
            self.logger.log_info(f'create_table_log error: NO FILES in {log_data_path}')
            return
        
        try:
            # Use read_csv_auto to read all CSVs at once (improves efficiency)
            file_paths_str = ', '.join([f"'{file}'" for file in log_csv_files])
            
            # Load all the CSV files into a single table
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE raw.log_table AS
                SELECT * FROM read_csv_auto([{file_paths_str}]);
            """)
            
            self.logger.log_info(f"Successfully created and populated raw.log_table with {len(log_csv_files)} files.")
        except Exception as e:
            self.logger.log_info(f"Error creating log table: {e}")
            return
    
    def create_line_table(self):
        line_data_path = f"{BASE_PATH}/app/data/processed/lines/"
        lines_csv_files = glob.glob(f'{line_data_path}*.csv')

        if not lines_csv_files:
            self.logger.log_info(f'create_line_table error: NO FILES in {line_data_path}')
            return

        try:
            # Use read_csv_auto to read all CSVs at once (improves efficiency)
            file_paths_str = ', '.join([f"'{file}'" for file in lines_csv_files])
            
            # Load all the CSV files into a single table
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE raw.lines_table AS
                SELECT * FROM read_csv_auto([{file_paths_str}]);
            """)

            self.logger.log_info(f"Successfully created and populated raw.lines_table with {len(lines_csv_files)} files.")
        except Exception as e:
            self.logger.log_info(f"Error creating lines table: {e}")
            return
    
    def create_stat_tables(self):
        # Iterate over 'teams' and 'players' types
        for tp in ['teams', 'players']:
            for kind in self.CURRENT_TABLES:
                try:
                    # Build the path to the CSV files
                    current_path = f'{DATA_PATH}/{tp}/{kind}/'
                    csv_files = glob.glob(f'{current_path}*.csv')
                    
                    if not csv_files:
                        self.logger.log_error(f"create_stat_tables error for {tp}, {kind}: No files in {current_path}")
                        continue
                    
                    # Use read_csv_auto to read all CSVs at once (improves efficiency)
                    file_paths_str = ', '.join([f"'{file}'" for file in csv_files])
                    
                    # Create the table and populate it using all the CSVs in one operation
                    self.conn.execute(f"""
                        CREATE OR REPLACE TABLE raw.{tp}_{kind} AS
                        SELECT * FROM read_csv_auto([{file_paths_str}], union_by_name=true, files_to_sniff=-1);
                    """)

                    self.logger.log_info(f"Successfully created and populated table raw.{tp}_{kind} with {len(csv_files)} files.")
                    
                except Exception as e:
                    self.logger.log_error(f"Error creating table {tp}_{kind}: {e}")
                    continue  # Continue processing other tables even if this one fails
                    
        self.logger.log_info("Finished creating all stat tables.")

    
    def repopulate_db(self):
        if not self.schema_exists('raw'):
            self.conn.execute(f"""
                CREATE SCHEMA raw;
                """).df()
        self.create_log_table()
        self.create_line_table()
        self.create_stat_tables()
        self.logger.log_info(f"done repopulating database")



    
def main():
    logger = Logger()
    new = TableGenerator(logger)
    # print(new.CURRENT_TABLES)
    new.repopulate_db()



if __name__ == "__main__":
    main()