import pandas as pd
import glob
import duckdb
import os
# import sqlmesh

from ..utils.logger import Logger


# Define the base path relative to the project root
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# Your data path
DATA_PATH = os.path.join(BASE_PATH, 'app', 'data', 'raw')
DATABASE_PATH = os.path.join(BASE_PATH, 'app', 'database')

SQLMESH_PATH = os.path.join(BASE_PATH, 'app', 'sql', 'sqlmesh')
# print(f"Data Path: {SQLMESH_PATH}")


# Just a check to confirm paths
print(f"Base Path: {BASE_PATH}")
print(f"Data Path: {DATA_PATH}")
print(f"Database Path: {DATABASE_PATH}")


DATA_TYPE_MAPPINGS = {
    'int64': 'BIGINT',
    'object': 'TEXT',
    'float64': 'DOUBLE'
}

PRIMARY_KEYS = {
    'players': ('GAME_ID', 'PLAYER_ID'),
    'teams': ('GAME_ID', 'TEAM_ID'),
    'log': ('GAME_ID', 'TEAM_ID'),
    'lines': ('GAME_ID', 'TEAM_ABBREVIATION')
}


class TableGenerator:

    def __init__(self, logger):
        self.conn = duckdb.connect(f'{DATABASE_PATH}/nba.db')
        self.endpoints = self.get_endpoints()
        self.tables_to_create = self.get_table_paths()
        self.logger = logger

    def get_endpoints(self):
        """Fetch common endpoints from both teams and players directories."""
        team_table_paths = glob.glob(f'{DATA_PATH}/teams/*')
        team_tables = set([x.split(os.sep)[-1] for x in team_table_paths])

        player_table_paths = glob.glob(f'{DATA_PATH}/players/*')
        player_tables = set([x.split(os.sep)[-1] for x in player_table_paths])

        return team_tables.intersection(player_tables)

    def get_table_paths(self):
        """Get paths for the tables to be created."""
        table_paths = []

        # Add team and player tables paths
        for ep in self.endpoints:
            table_paths.append(f'{DATA_PATH}/teams/{ep}/')
            table_paths.append(f'{DATA_PATH}/players/{ep}/')

        # Always include log and lines directories
        table_paths.append(f'{DATA_PATH}/log/')
        table_paths.append(f'{DATA_PATH}/lines/')
        
        return table_paths

    def schema_exists(self, schema_name):
        """Check if a schema exists using information_schema."""
        message = f"""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name = '{schema_name}'
        """
        result = self.conn.execute(message).fetchall()
        return len(result) > 0

    def create_table_from_csv(self, current_data_path):
        """Creates the table from CSV files in the specified path."""
        csv_files = glob.glob(f'{current_data_path}*.csv')

        if not csv_files:
            self.logger.log_error(f'NO CSV FILES FOUND FOR PATH {current_data_path}')
            return

        # Extract schema, table_class, and table_name from the file path
        csv_split = csv_files[0].split(os.sep)
        if 'teams' in csv_split or 'players' in csv_split:
            schema, table_class, table_name = csv_split[-4], csv_split[-3], csv_split[-2]
        else:
            schema, table_class, table_name = csv_split[-3], csv_split[-2], 'table'

        full_table_name = f"{schema}.{table_class}_{table_name}"
        if not self.schema_exists(schema):
            create_schema = f"CREATE SCHEMA {schema};"
            self.conn.execute(create_schema)
            self.logger.log_sql(create_schema)

        # Read sample data from the CSV file
        sample = pd.read_csv(csv_files[0])

        # Get table creation statements and external model definition
        table_creation_statement, external_model_definition = self.create_table_definitions(
            sample, schema, table_class, table_name)

        file_paths_str = ', '.join([f"'{file}'" for file in csv_files])
        insert_statement = f"""
            INSERT INTO {full_table_name}
            (SELECT * FROM read_csv_auto([{file_paths_str}], union_by_name=true, files_to_sniff=-1,nullstr=''));
        """

        try:
            # Execute table creation and insertion
            self.conn.execute(table_creation_statement)

            self.conn.execute(insert_statement)

            self.logger.log_sql(table_creation_statement)
            self.logger.log_sql(insert_statement)


            self.logger.log_info(f"Successfully created and populated {full_table_name} with {len(csv_files)} files.")
        except Exception as e:
            self.logger.log_warning(f"Error creating / inserting table {full_table_name}: {e}")
            return

        # Write the external model definition to a YAML file
        yaml_path = os.path.join(SQLMESH_PATH, 'models', 'external_models', f"{schema}_{table_class}_{table_name}.yaml")
        with open(yaml_path, 'w') as f:
            f.write(external_model_definition)

        return table_creation_statement

    def create_table_definitions(self, sample, schema, table_class, table_name):
        """Generate table creation SQL and the corresponding external model definition."""
        full_table_name = f"{schema}.{table_class}_{table_name}"
        table_creation_statement = f"CREATE OR REPLACE TABLE {full_table_name} (\n"
        external_model_definition = f"""- name: '"{schema}"."{table_class}_{table_name}"'\n  primary_key:"""

        # Add primary keys
        for pk in PRIMARY_KEYS.get(table_class, []):
            external_model_definition += f"\n    - {pk}"

        external_model_definition += f"\n  columns:"

        cols = sample.columns
        types = sample.dtypes

        # Create table columns based on the CSV data types
        for col, c_type in zip(cols, types):
            duckdb_type = DATA_TYPE_MAPPINGS.get(str(c_type), 'TEXT')  # Default to TEXT if unknown
            table_creation_statement += f"\t\"{col}\" {duckdb_type},\n"
            external_model_definition += f"\n    {col}: {duckdb_type}"

        table_creation_statement += f"\n);"
        external_model_definition += "\n  gateway: duckdb"

        return table_creation_statement, external_model_definition


def main():
    logger = Logger()
    logger.log_info(f"MAKING ALL TABLES FROM RAW FILES")
    new = TableGenerator(logger)
    for pathway in new.tables_to_create:
        new.create_table_from_csv(pathway)
    logger.log_info(f"DONE MAKING {len(new.tables_to_create)} tables")


if __name__ == "__main__":
    main()
