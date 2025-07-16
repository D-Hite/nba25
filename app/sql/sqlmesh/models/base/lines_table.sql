MODEL (
  name base.lines_table,
  kind FULL
);

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
             
            from raw.lines_table
             )
             
            select
             lt.GAME_ID::int as GAME_ID,
             lt.GAME_DATE,
             lt.TEAM_ABBREVIATION,
             rd.line as LINE,
             rd.total as OU

             from RAW_DATA rd
             join raw.LINE_TEAM_MAPPING_TABLE mp
                on mp.raw_data_team_name = rd.team
             inner join raw.log_table lt
             on lt.GAME_DATE::DATE == rd.P_DATE
             and lt.TEAM_NAME = mp.log_table_team_name
;
