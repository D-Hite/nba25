import pandas as pd
import glob
import duckdb
import os
# import sqlmesh

from ..utils.logger import Logger


# Define the base path relative to the project root
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# Your data path
DATA_PATH = os.path.join(BASE_PATH,  'app','data', 'raw')
DATABASE_PATH = os.path.join(BASE_PATH, 'app','database')


SQLMESH_PATH = os.path.join(BASE_PATH, 'app', 'sql','sqlmesh')
# print(f"Data Path: {SQLMESH_PATH}")


# Just a check to confirm paths
print(f"Base Path: {BASE_PATH}")
print(f"Data Path: {DATA_PATH}")
print(f"Data Path: {DATABASE_PATH}")


DATA_TYPE_MAPPINGS = {'int64':'BIGINT',
                      'object':'TEXT',
                      'float64':'DOUBLE'}



class TableGenerator():

    def __init__(self, logger):
        self.conn = duckdb.connect(f'{DATABASE_PATH}/nba.db')
        self.endpoints =  self.get_endpoints()
        self.tables_to_create = self.get_table_paths()
        self.logger = logger

    def get_endpoints(self):
        team_table_paths = glob.glob(f'{DATA_PATH}/teams/*')
        team_tables = set([x.split('/')[-1] for x in team_table_paths])
        print(team_tables)

        player_table_paths = glob.glob(f'{DATA_PATH}/players/*')
        player_tables = set([x.split('/')[-1] for x in player_table_paths])

        return team_tables.intersection(player_tables)
    
    def get_table_paths(self):
        table_paths = []
        for ep in self.endpoints:
            table_paths.append(f'{DATA_PATH}/teams/{ep}/')
        table_paths.append(f'{DATA_PATH}/log/')
        table_paths.append(f'{DATA_PATH}/lines/')
        return table_paths

    


    def schema_exists(self, schema_name):
        """Check if a schema exists using information_schema."""
        result = self.conn.execute(f"""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name = '{schema_name}'
        """).fetchall()
        return len(result) > 0
    
    def create_external_model_all(self, table_name, schema_name, db_name='nba'):

        external_model_file = f"{SQLMESH_PATH}/external_models.yaml"
        file_exists = os.path.exists(external_model_file)
        mode = 'a' if file_exists else 'w'


        if file_exists:
            #check for external model
            with open(external_model_file, 'r') as f:
                lines = f.readlines()
                current_table_exist = [x for x in lines if f"""- name: '"{db_name}"."{schema_name}"."{table_name}"'""" in x]
                if len(current_table_exist):
                    message = f"TABLE {db_name}.{schema_name}.{table_name} already exists in external_models.yaml"
                    ### re define table??
                    return 
                print(current_table_exist)
        
        df = self.conn.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{table_name}';
        """).fetchdf()

        write_string = f"""- name: \'"{db_name}"."{schema_name}"."{table_name}"\'\n  columns:"""

        for col, d_type in zip(df['column_name'],df['data_type']):
            write_string+=f"\n    {col}: {d_type}"
        
        write_string+="\n  gateway: duckdb"

        print(write_string)
        

        with open(external_model_file, mode) as f2:
            f2.write(write_string)

        return 1

    def create_external_model(self, table_name, schema_name, db_name='nba'):
        external_model_file = f"{SQLMESH_PATH}/external_models/{table_name}.yaml"
        file_exists = os.path.exists(external_model_file)
        ## ensure location exists
        os.makedirs(os.path.dirname(external_model_file), exist_ok=True)

        df = self.conn.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{table_name}';
        """).fetchdf()

        if not df.empty:

            write_string = f"""- name: \'"{db_name}"."{schema_name}"."{table_name}"\'\n  columns:"""
            write_string+="\n  gateway: duckdb"

            for col, d_type in zip(df['column_name'],df['data_type']):
                write_string+=f"\n    {col}: {d_type}"
            
            ## write primary keys too
            
            
            print(write_string)
            

            with open(external_model_file, mode) as f2:
                f2.write(write_string)
        else:
            self.logger.log_warning()



    def create_table_from_csv(self, current_data_path):
        csv_files = glob.glob(f'{current_data_path}*.csv')

        if not csv_files:
            self.logger.log_error(f'NO CSV FILES FOUND FOR PATH {current_data_path}')

        sample = pd.read_csv(csv_files[0])
        print(sample.columns)
        print(sample.dtypes)

        ### FOR COL IN COLUMNS, MAP DTYPES TO DUCKDB TYPES, DEFINE TABLE COLUMNS AND PK's, INSERT FROM READ_CSV_AUTO

        table_creation_statement = "CREATE"

        # for col in sample.columns:
            
        



        try:
            # Use read_csv_auto to read all CSVs at once (improves efficiency)
            file_paths_str = ', '.join([f"'{file}'" for file in csv_files])
            # print(file_paths_str)
            
            
            # self.logger.log_info(f"Successfully created and populated raw.log_table with {len(log_csv_files)} files.")
        except Exception as e:
            # self.logger.log_info(f"Error creating log table: {e}")
            return



    
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
                (SELECT * FROM read_csv_auto([{file_paths_str}]));
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
                SELECT * FROM read_csv_auto([{file_paths_str}])
                PRIMARY KEY (GAME_ID, TEAM_ABBREVIATION);
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
                    if tp == 'players':
                        self.conn.execute(f"""
                            CREATE OR REPLACE TABLE raw.{tp}_{kind} AS
                            SELECT * FROM read_csv_auto([{file_paths_str}], union_by_name=true, files_to_sniff=-1)
                            PRIMARY KEY (GAME_ID, PLAYER_ID);
                        """)
                    else:
                        self.conn.execute(f"""
                            CREATE OR REPLACE TABLE raw.{tp}_{kind} AS
                            SELECT * FROM read_csv_auto([{file_paths_str}], union_by_name=true, files_to_sniff=-1)
                            PRIMARY KEY (GAME_ID, TEAM_ID);
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
        # self.create_line_table()
        # self.create_stat_tables()
        self.logger.log_info(f"done repopulating database")



    
def main():
    logger = Logger()
    new = TableGenerator(logger)
    print(new.tables_to_create)
    new.create_table_from_csv(f"{DATA_PATH}/log/")
    # new.create_log_table()


    # new.repopulate_db()
    # new.create_external_model('teams_misc', 'raw')
    # sqlmesh.cli.main.create_external_models()



if __name__ == "__main__":
    main()