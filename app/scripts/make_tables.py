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

PRIMARY_KEYS = {'players':('GAME_ID', 'PLAYER_ID'),
                'teams':('GAME_ID', 'TEAM_ID'),
                'log':('GAME_ID', 'TEAM_ID'),
                'lines':('GAME_ID', 'TEAM_ABBREVIATION')}



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
        for ep in self.endpoints:
            table_paths.append(f'{DATA_PATH}/players/{ep}/')
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


    def create_table_from_csv(self, current_data_path):
        csv_files = glob.glob(f'{current_data_path}*.csv')

        if not csv_files:
            self.logger.log_error(f'NO CSV FILES FOUND FOR PATH {current_data_path}')

        # get table and schema
        csv_split = csv_files[0].split('/')
        if 'teams' in csv_split or 'players' in csv_split:
            schema, table_class, table_name = csv_split[-4],csv_split[-3], csv_split[-2]
        else:
            schema, table_class, table_name = csv_split[-3],csv_split[-2],'table'
        full_table_name = f"{schema}.{table_class}_{table_name}"

        sample = pd.read_csv(csv_files[0])

        table_creation_statement, external_model_definition = self.create_table_definitions(sample,schema,table_class,table_name)

        file_paths_str = ', '.join([f"'{file}'" for file in csv_files])          
        insert_statement = f"""
                        INSERT INTO {full_table_name}
                        (SELECT * FROM read_csv_auto([{file_paths_str}], union_by_name=true, files_to_sniff=-1));
                        """
        try:
            self.conn.execute(table_creation_statement)
            self.conn.execute(insert_statement)
            
            self.logger.log_info(f"Successfully created and populated {full_table_name} with {len(csv_files)} files.")
        except Exception as e:
            self.logger.log_warning(f"Error creating / inserting table {full_table_name}: {e}")
            return
        
        with open(f"{SQLMESH_PATH}/models/external_models/{schema}_{table_class}_{table_name}.yaml", 'w') as f:
            f.write(external_model_definition)
        

        return table_creation_statement

    
    def create_table_definitions(self,sample,schema,table_class,table_name):
        full_table_name = f"{schema}.{table_class}_{table_name}"
        table_creation_statement = f"CREATE OR REPLACE TABLE {full_table_name} (\n"

        ### FOR COL IN COLUMNS, MAP DTYPES TO DUCKDB TYPES, DEFINE TABLE COLUMNS AND PK's, INSERT FROM READ_CSV_AUTO

        table_creation_statement = f"CREATE OR REPLACE TABLE {full_table_name} (\n"
        external_model_definition = f"""- name: \'"nba"."{schema}"."{table_class}_{table_name}"\'\n  primary_key:"""
        for pk in PRIMARY_KEYS[table_class]:
            external_model_definition += f"\n    - {pk}"
        external_model_definition += f"\n  columns:"

        cols = sample.columns
        types = sample.dtypes
        for col, c_type in zip(cols, types):
            if 'DATE' in col:
                table_creation_statement+=f"\t\"{col}\" DATE,\n"
                external_model_definition+=f"\n    {col}: DATE"
            # elif col in PRIMARY_KEYS[table_class]:
            #     table_creation_statement+=f"\t{col} {DATA_TYPE_MAPPINGS[str(c_type)]} PRIMARY KEY,\n"
            else:
                table_creation_statement+=f"\t\"{col}\" {DATA_TYPE_MAPPINGS[str(c_type)]},\n"
                external_model_definition+=f"\n    {col}: {DATA_TYPE_MAPPINGS[str(c_type)]}"
        
        # table_creation_statement+=f"\tPRIMARY_KEY ({PRIMARY_KEYS[table_class]})"

        table_creation_statement+=f"\n);"
        external_model_definition+="\n  gateway: duckdb"

        return table_creation_statement,external_model_definition


    
def main():
    logger = Logger()
    new = TableGenerator(logger)
    for pathway in new.tables_to_create:
        new.create_table_from_csv(pathway)


if __name__ == "__main__":
    main()