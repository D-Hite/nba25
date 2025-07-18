MODEL (
  name base.player_sums,
  kind FULL
);

SELECT
    PLAYER_ID,
    SEASON_ID,
    TEAM_ID,
    GAME_ID,
    NEXT_GAME_ID,
    GAME_DATE,
    PLAYER_NAME,
    TEAM_NAME,

    -- Cumulative Games Played (up to and including this game)
    GAME_COUNT,
    GAMES_PLAYED,
    PLAYED_FLAG,
    IS_LAST_GAME,
    -- Cumulative Minutes
    SUM(
        MINUTES
    ) OVER (
        PARTITION BY PLAYER_ID, SEASON_ID
        ORDER BY CAST(GAME_DATE AS DATE)
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS CUM_MINUTES,

    -- Cumulative Points

        
SUM(PTS) OVER (
                PARTITION BY PLAYER_ID, SEASON_ID
                ORDER BY CAST(GAME_DATE AS DATE)
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS CUM_PTS,
        
        
SUM(AST) OVER (
                PARTITION BY PLAYER_ID, SEASON_ID
                ORDER BY CAST(GAME_DATE AS DATE)
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS CUM_AST,
        
        
SUM(REB) OVER (
                PARTITION BY PLAYER_ID, SEASON_ID
                ORDER BY CAST(GAME_DATE AS DATE)
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS CUM_REB,
        
        
SUM(OREB) OVER (
                PARTITION BY PLAYER_ID, SEASON_ID
                ORDER BY CAST(GAME_DATE AS DATE)
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS CUM_OREB,
        
        
SUM(DREB) OVER (
                PARTITION BY PLAYER_ID, SEASON_ID
                ORDER BY CAST(GAME_DATE AS DATE)
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS CUM_DREB,
        
        
SUM(STL) OVER (
                PARTITION BY PLAYER_ID, SEASON_ID
                ORDER BY CAST(GAME_DATE AS DATE)
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS CUM_STL,
        
        
SUM(BLK) OVER (
                PARTITION BY PLAYER_ID, SEASON_ID
                ORDER BY CAST(GAME_DATE AS DATE)
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS CUM_BLK,
        
        
SUM(PF) OVER (
                PARTITION BY PLAYER_ID, SEASON_ID
                ORDER BY CAST(GAME_DATE AS DATE)
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS CUM_PF,


SUM("TO") OVER (
                PARTITION BY PLAYER_ID, SEASON_ID
                ORDER BY CAST(GAME_DATE AS DATE)
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS CUM_TO,
        
        
SUM(FGM) OVER (
                PARTITION BY PLAYER_ID, SEASON_ID
                ORDER BY CAST(GAME_DATE AS DATE)
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS CUM_FGM,
        
        
SUM(FGA) OVER (
                PARTITION BY PLAYER_ID, SEASON_ID
                ORDER BY CAST(GAME_DATE AS DATE)
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS CUM_FGA,
        
        
SUM(FG3M) OVER (
                PARTITION BY PLAYER_ID, SEASON_ID
                ORDER BY CAST(GAME_DATE AS DATE)
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS CUM_FG3M,
        
        
SUM(FG3A) OVER (
                PARTITION BY PLAYER_ID, SEASON_ID
                ORDER BY CAST(GAME_DATE AS DATE)
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS CUM_FG3A,
        
        
SUM(FTM) OVER (
                PARTITION BY PLAYER_ID, SEASON_ID
                ORDER BY CAST(GAME_DATE AS DATE)
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS CUM_FTM,
        
        
SUM(FTA) OVER (
                PARTITION BY PLAYER_ID, SEASON_ID
                ORDER BY CAST(GAME_DATE AS DATE)
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS CUM_FTA,
        
        
SUM(PLUS_MINUS) OVER (
                PARTITION BY PLAYER_ID, SEASON_ID
                ORDER BY CAST(GAME_DATE AS DATE)
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS CUM_PLUS_MINUS,
        
        
SUM(PTS_OFF_TOV) OVER (
                PARTITION BY PLAYER_ID, SEASON_ID
                ORDER BY CAST(GAME_DATE AS DATE)
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS CUM_PTS_OFF_TOV,
        
        
SUM(PTS_2ND_CHANCE) OVER (
                PARTITION BY PLAYER_ID, SEASON_ID
                ORDER BY CAST(GAME_DATE AS DATE)
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS CUM_PTS_2ND_CHANCE,
        
        
SUM(PTS_FB) OVER (
                PARTITION BY PLAYER_ID, SEASON_ID
                ORDER BY CAST(GAME_DATE AS DATE)
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS CUM_PTS_FB,
        
        
SUM(PTS_PAINT) OVER (
                PARTITION BY PLAYER_ID, SEASON_ID
                ORDER BY CAST(GAME_DATE AS DATE)
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS CUM_PTS_PAINT,
        
        
SUM(OPP_PTS_OFF_TOV) OVER (
                PARTITION BY PLAYER_ID, SEASON_ID
                ORDER BY CAST(GAME_DATE AS DATE)
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS CUM_OPP_PTS_OFF_TOV,
        
        
SUM(OPP_PTS_2ND_CHANCE) OVER (
                PARTITION BY PLAYER_ID, SEASON_ID
                ORDER BY CAST(GAME_DATE AS DATE)
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS CUM_OPP_PTS_2ND_CHANCE,
        
        
SUM(OPP_PTS_FB) OVER (
                PARTITION BY PLAYER_ID, SEASON_ID
                ORDER BY CAST(GAME_DATE AS DATE)
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS CUM_OPP_PTS_FB,
        
        
SUM(OPP_PTS_PAINT) OVER (
                PARTITION BY PLAYER_ID, SEASON_ID
                ORDER BY CAST(GAME_DATE AS DATE)
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS CUM_OPP_PTS_PAINT,
        
        
SUM(BLKA) OVER (
                PARTITION BY PLAYER_ID, SEASON_ID
                ORDER BY CAST(GAME_DATE AS DATE)
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS CUM_BLKA,
        
        
SUM(PFD) OVER (
                PARTITION BY PLAYER_ID, SEASON_ID
                ORDER BY CAST(GAME_DATE AS DATE)
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS CUM_PFD,
        

FROM base.players_processed
-- WHERE PLAYER_ID IS NOT NULL
ORDER BY PLAYER_ID, SEASON_ID, CAST(GAME_DATE AS DATE)
;
