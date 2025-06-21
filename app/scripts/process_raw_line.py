"""
The purpose of this script is to join betting data with nba.stats game_ids so games can easily be joined together

raw line data is from this website:
https://sportsdatabase.com/NBA/query.html

using this query
f"date, team, site, o:team, line, total @season={YYYY}"

"""

# %%
import pandas as pd
import glob
import time
import duckdb





# %%
conn = duckdb.connect('firstdb.db')


# %%
"""
first need to make a mapping table to map team names from raw files(sportsdatabase.com) to team names from log files (nba.stats)

"""
conn.execute("""CREATE OR REPLACE TABLE LINE_TEAM_MAPPING_TABLE
             (raw_data_team_name VARCHAR,
             log_table_team_name VARCHAR)""").df()

conn.execute("""INSERT INTO LINE_TEAM_MAPPING_TABLE (raw_data_team_name,log_table_team_name)
             VALUES
('Jazz','Utah Jazz'),
('Wizards','Washington Wizards'),
('Spurs','San Antonio Spurs'),
('Hornets','Charlotte Hornets'),
('Bucks','Milwaukee Bucks'),
('Grizzlies','Memphis Grizzlies'),
('Hawks','Atlanta Hawks'),
('Pacers','Indiana Pacers'),
('Pistons','Detroit Pistons'),
('Timberwolves','Minnesota Timberwolves'),
('Seventysixers','Philadelphia 76ers'),
('Nuggets','Denver Nuggets'),
('Thunder','Oklahoma City Thunder'),
('Celtics','Boston Celtics'),
('Raptors','Toronto Raptors'),
('Cavaliers','Cleveland Cavaliers'),
('Warriors','Golden State Warriors'),
('Knicks','New York Knicks'),
('Mavericks','Dallas Mavericks'),
('Lakers','Los Angeles Lakers'),
('Nets','Brooklyn Nets'),
('Magic','Orlando Magic'),
('Heat','Miami Heat'),
('Trailblazers','Portland Trail Blazers'),
('Suns','Phoenix Suns'),
('Clippers','LA Clippers'),
('Rockets','Houston Rockets'),
('Bulls','Chicago Bulls'),
('Kings','Sacramento Kings'),
('Pelicans','New Orleans Pelicans')""").df()





# %%
"""make processed file for each 'raw' file"""
raw_line_filepath = 'DATA/raw/lines/'
lines_csv_files = glob.glob(f'{raw_line_filepath}*.csv')
if not lines_csv_files:
    print(f'create_line_table error: no file in {raw_line_filepath}')

for file in lines_csv_files:
    try:
        conn.execute(f"""
            CREATE OR REPLACE TABLE raw_line_table as
            SELECT
            date::VARCHAR as date
            ,team::VARCHAR as team
            ,site::VARCHAR as ha
            ,'o:team'::VARCHAR as other_team
            ,TRY_CAST(line AS DOUBLE) as line
            ,TRY_CAST(total AS DOUBLE) as total

            FROM read_csv_auto('{file}')
        """).df()
    except Exception as e:
        print(f"error: {e}")


    new_data = conn.execute("""     
            WITH RAW_DATA AS (
             
            SELECT CAST(
            SUBSTRING(CAST(date AS VARCHAR), 1, 4) || '-' || 
            SUBSTRING(CAST(date AS VARCHAR), 5, 2) || '-' || 
            SUBSTRING(CAST(date AS VARCHAR), 7, 2)
            as DATE
            )
            AS P_DATE,
            
            team,
            line,
            total,
             
            from raw_line_table
             )
             
            select
             lt.GAME_ID::int as GAME_ID,
             lt.GAME_DATE,
             lt.TEAM_ABBREVIATION,
             rd.line as LINE,
             rd.total as OU

             from RAW_DATA rd
             join LINE_TEAM_MAPPING_TABLE mp
                on mp.raw_data_team_name = rd.team
             inner join log_table lt
             on lt.GAME_DATE::DATE == rd.P_DATE
             and lt.TEAM_NAME = mp.log_table_team_name   
             
             """).df()
    new_data.to_csv(f"Data/lines/LinesV1{file[-11:-4]}.csv",index=False)
    print(f"WROTE FILE: Data/lines/LinesV1{file[-11:-4]}.csv")
    conn.execute("drop table if exists raw_line_table")


# %%
conn.close()
# %%
