select 
  GAME_ID,
  game_date,
  TEAM_ABBREVIATION,
  MATCHUP,
  WL,
  pts,
  PLUS_MINUS,
  LINE,
  OU,
          -- Extracting home and away team info
        CASE 
            WHEN MATCHUP LIKE '%vs.%' THEN SUBSTRING(MATCHUP, 0, 4)
            ELSE SUBSTRING(MATCHUP, 7)
        END AS HOME_TEAM,
        
        CASE 
            WHEN MATCHUP LIKE '%vs.%' THEN SUBSTRING(MATCHUP, 9, 4)
            ELSE SUBSTRING(MATCHUP, 0, 4)
        END AS AWAY_TEAM,
        
        -- To check if the team is home or away
        CASE 
            WHEN MATCHUP LIKE '%vs.%' AND SUBSTRING(MATCHUP, 0, 4) = TEAM_ABBREVIATION THEN 'Home'
            WHEN MATCHUP LIKE '%vs.%' AND SUBSTRING(MATCHUP, 9, 4) = TEAM_ABBREVIATION THEN 'Away'
            WHEN MATCHUP NOT LIKE '%vs.%' AND SUBSTRING(MATCHUP, 0, 4) = TEAM_ABBREVIATION THEN 'Away'
            WHEN MATCHUP NOT LIKE '%vs.%' AND SUBSTRING(MATCHUP, 7) = TEAM_ABBREVIATION THEN 'Home'
        END AS HOME_AWAY_STATUS
  
from base.teams_combined
  where GAME_ID = 0021300004;