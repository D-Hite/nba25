import pandas as pd
import glob
import time
import duckdb
import os

from ..utils.logger import Logger

## import sqlmesh stuff or just write out to sqlmesh model folder

# Define the base path relative to the project root
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# Your data path inside the 'collection' directory
DATA_PATH = os.path.join(BASE_PATH,  'app','data', 'raw')
DATABASE_PATH = os.path.join(BASE_PATH, 'app','database')
SQLMESH_PATH = os.path.join(BASE_PATH, 'app', 'sqlmesh')
SQL_PATH = os.path.join(BASE_PATH, 'app', 'sql','sql')


# print(f"Data Path: {SQLMESH_PATH}")
# Just a check to confirm paths
# print(f"Base Path: {BASE_PATH}")
# print(f"Data Path: {DATA_PATH}")
# print(f"Data Path: {DATABASE_PATH}")


class  SQLMeshModelGenerator():

    def __init__(self, logger):
        self.logger = logger
    
    def create_model(self, schema, sql_code):
        return None



class SQLGenerator():

    def __init__(self, logger):
        self.logger = logger
        self.conn = duckdb.connect(f'{DATABASE_PATH}/nba.db')
    

    def schema_exists(self, schema_name):
        """Check if a schema exists using information_schema."""
        result = self.conn.execute(f"""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name = '{schema_name}'
        """).fetchall()
        return len(result) > 0

    def write_sql(self, schema, name, sql):
        schema_dir = os.path.join(SQL_PATH, schema)
        os.makedirs(schema_dir, exist_ok=True)
        if not self.schema_exists(schema):
            self.logger.log_info(f'CREATING NEW SCHEMA: {schema}')
            self.conn.execute(f"""
                CREATE SCHEMA {schema};
                """).df()
        self.logger.log_info(f'writing new table {schema}.{name}')
        with open(os.path.join(SQL_PATH, schema, name+'.sql'), 'w') as f:
            f.write(f"""CREATE OR REPLACE TABLE {schema}.{name} AS\n{sql}""")
            self.conn.execute(f"""CREATE OR REPLACE TABLE {schema}.{name} AS\n{sql}""")


    def make_sql_model(self,schema,name,sql,kind='FULL'):
        schema_dir = os.path.join(SQLMESH_PATH, 'models', schema)
        os.makedirs(schema_dir, exist_ok=True)
        with open(os.path.join(SQLMESH_PATH, 'models', schema, name+'.sql'), 'w') as f:
            f.write(f"""MODEL (
                    name {schema}.{name},
                    kind FULL
                    );
                    {sql}""")



class CombinedGenerator(SQLGenerator):


    def get_column_sources(self,cols):
        """
        used to select which columns come from what source

        cols:pandas.DataFrame, EX:self.conn.execute("SELECT table_name, column_name FROM information_schema.columns WHERE table_name ilike '%players%' or table_name = 'log_table'").df()
        player_data: True if player data
        """
        col_dict = dict()
        ## FIRST GET ALL COLUMNS IN DICTIONARY
        for tn, cn, in zip(cols['table_name'],cols['column_name']):
            if cn not in col_dict.keys():
                col_dict[cn] = []
            col_dict[cn].append(tn)

        return col_dict

    def sql_create_player_combination(self,col_dict):
        """
        reads through col_dict generated from get_column_sources to make the players_combined 
        updated to coalesce values when there are multiple tables with the column
        
        """

        order_map = {'log_table':0,'players_fourfactors':1}

        spec_col_set = set()
        for value_list in col_dict.values():
            spec_col_set.update(value_list)
        spec_col_set.discard('log_table')

        col_sql = ""
        for col_name in col_dict.keys():
            if col_name == 'TOV':### THIS IS ONLY IN LOG TABLE and doesnt apply to player data
                continue
            # og_tables = col_dict[col_name]
            col_dict[col_name] = sorted(col_dict[col_name], key=lambda x: order_map.get(x, float('inf')))
            if col_name in ['SEASON_ID','TEAM_ID','TEAM_ABBREVIATION','TEAM_NAME','GAME_ID','GAME_DATE','MATCHUP','WL']:## PLAYER DATA LOG DATA
                col_dict[col_name] = ['log_table']
            elif 'log_table' in col_dict[col_name]:
                col_dict[col_name] = col_dict[col_name][1:]
            if len(col_dict[col_name]) == 1:
                # print(f"COLUMN:{col_name}, TABLES: {og_tables}, statement: {col_dict[col_name][0]}.{col_name},")
                col_sql+=f"\n\traw.{col_dict[col_name][0]}.{col_name},"
            else:
                coalesce_statement = ""
                for table_name in col_dict[col_name]:
                    coalesce_statement+=f"raw.{table_name}.{col_name},"
                coalesce_statement = coalesce_statement[:-1]
                # print(f"COLUMN:{col_name}, TABLES: {og_tables}, statement: COALESCE({coalesce_statement}) as {col_name},")
                col_sql+=f"\n\tCOALESCE({coalesce_statement}) as {col_name},"

        join_sql = "FROM raw.log_table"
        first = True
        last_table = 'log_table'
        for i in spec_col_set:
            if first:
                join_sql+=f"\nleft join raw.{i} on raw.{last_table}.GAME_ID::int = raw.{i}.GAME_ID::int and raw.{last_table}.TEAM_ABBREVIATION = raw.{i}.TEAM_ABBREVIATION"
                first = False
                last_table = i
            else:
                join_sql+=f"\nleft join raw.{i} on raw.{last_table}.GAME_ID::int = raw.{i}.GAME_ID::int and raw.{last_table}.PLAYER_NAME = raw.{i}.PLAYER_NAME"
                last_table=i

        combined_f_string =f"""SELECT DISTINCT
            {col_sql}
        {join_sql}"""

        return combined_f_string



    def sql_create_team_combination(self,col_dict):

        order_map = {'log_table':0,'teams_fourfactors':1}

        spec_col_set = set()
        for value_list in col_dict.values():
            spec_col_set.update(value_list)
        spec_col_set.discard('log_table')

        col_sql = ""
        for col_name in col_dict.keys():
            col_dict[col_name] = sorted(col_dict[col_name], key=lambda x: order_map.get(x, float('inf')))
            if len(col_dict[col_name]) == 1:
                # print(f"COLUMN:{col_name}, TABLES: {og_tables}, statement: {col_dict[col_name][0]}.{col_name},")
                col_sql+=f"\n\traw.{col_dict[col_name][0]}.{col_name},"
            else:
                coalesce_statement = ""
                if col_name in ['GAME_ID','MIN']:### COLUMNS WITH DIFFERENT TYPES: TODO: MAKE THIS AUTOMATIC?
                    for table_name in col_dict[col_name]:
                        coalesce_statement+=f"CAST(raw.{table_name}.{col_name} AS VARCHAR),"
                else:
                    for table_name in col_dict[col_name]:
                        coalesce_statement+=f"raw.{table_name}.{col_name},"
                coalesce_statement = coalesce_statement[:-1]
                # print(f"COLUMN:{col_name}, TABLES: {og_tables}, statement: COALESCE({coalesce_statement}) as {col_name},")
                col_sql+=f"\n\tCOALESCE({coalesce_statement}) as {col_name},"

        join_sql = "FROM raw.log_table"
        first = True
        last_table = 'log_table'
        for i in spec_col_set:
            join_sql+=f"\nleft join raw.{i} on raw.{last_table}.GAME_ID::int = raw.{i}.GAME_ID::int and raw.{last_table}.TEAM_ABBREVIATION = raw.{i}.TEAM_ABBREVIATION"
            # last_table=i

        combined_f_string =f"""
            SELECT DISTINCT
            {col_sql}
        {join_sql}"""

        return combined_f_string


    def generate_sql(self):
        playerandlog_columns = self.conn.execute("SELECT table_name, column_name FROM information_schema.columns WHERE (table_name ilike '%players%'  or table_name in ('log_table')) and table_schema = 'raw'").df()
        teamandlog_columns = self.conn.execute("SELECT table_name, column_name FROM information_schema.columns WHERE (table_name ilike '%teams%' or table_name in ('log_table', 'lines_table')) and table_schema = 'raw'").df()

        players_dict = self.get_column_sources(playerandlog_columns)
        player_sql = self.sql_create_player_combination(players_dict)

        team_dict = self.get_column_sources(teamandlog_columns)
        team_sql = self.sql_create_team_combination(team_dict)

        return {'teams_combined':team_sql,'players_combined':player_sql}



def main():
    logger = Logger()
    x = CombinedGenerator(logger)
    sql = x.generate_sql()
    for model_name in sql:
        x.write_sql('base',model_name,sql[model_name])
        

if __name__ == "__main__":
    main()