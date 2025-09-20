MODEL (
    name base.teams_processed,
    kind FULL
);

WITH base_data AS (
    SELECT
        *,
        CASE 
            WHEN season_id::VARCHAR ILIKE '4%' THEN 'PLAYOFFS'
            WHEN season_id::VARCHAR ILIKE '6%' THEN 'NBA_CUP'
            ELSE 'REGULAR'
        END AS SEASON_TYPE,
        COALESCE(PLUS_MINUS, 0) AS SCORE_DIFF,
        CASE WHEN MATCHUP ILIKE '% vs. %' THEN TRUE ELSE FALSE END AS IS_HOME,
        TRIM(
          CASE WHEN MATCHUP ILIKE '%vs.%' THEN SPLIT_PART(MATCHUP, 'vs.', 1)
               ELSE SPLIT_PART(MATCHUP, '@', 2)
          END
        ) AS HOME_TEAM,
        TRIM(
          CASE WHEN MATCHUP ILIKE '%vs.%' THEN SPLIT_PART(MATCHUP, 'vs.', 2)
               ELSE SPLIT_PART(MATCHUP, '@', 1)
          END
        ) AS AWAY_TEAM
    FROM base.teams_combined
),
with_opponent AS (
    SELECT
        t1.*,
        t2.PTS AS OPP_PTS
    FROM base_data t1
    JOIN base_data t2
        ON t1.GAME_ID = t2.GAME_ID
        AND t1.TEAM_ID != t2.TEAM_ID
),

outcomes AS (
    SELECT
        *,
        CASE 
            WHEN LINE is NULL then NULL
            WHEN IS_HOME AND SCORE_DIFF > -LINE THEN 'Cover'
            WHEN NOT IS_HOME AND SCORE_DIFF > LINE THEN 'Cover'
            ELSE 'No Cover'
        END AS COVER_RESULT,
        CASE
            WHEN OU IS NULL THEN NULL
            WHEN PTS + OPP_PTS > OU THEN 'Over'
            WHEN PTS + OPP_PTS < OU THEN 'Under'
            ELSE 'Push'
        END AS OU_RESULT
    FROM with_opponent
),

ranked_games AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY TEAM_ID, SEASON_ID 
            ORDER BY GAME_DATE
        ) AS GAME_NUMBER,
        MAX(GAME_DATE) OVER (
            PARTITION BY TEAM_ID, SEASON_ID
        ) AS LAST_TEAM_GAME_DATE,

        -- Days since last game
        DATE_DIFF(
            'day',
            CAST(LAG(GAME_DATE) OVER (
                PARTITION BY TEAM_ID, SEASON_ID
                ORDER BY GAME_DATE
            ) AS DATE),
            CAST(GAME_DATE AS DATE)
        ) AS DAYS_SINCE_LAST_GAME,


        -- Back-to-back flag
        CASE 
            WHEN DATE_DIFF(
                'day',
                CAST(LAG(GAME_DATE) OVER (
                    PARTITION BY TEAM_ID, SEASON_ID
                    ORDER BY GAME_DATE
                ) AS DATE),
                CAST(GAME_DATE AS DATE)
            ) = 1 THEN 1 ELSE 0
        END AS IS_BACK_TO_BACK,


        -- 3 games in 4 nights
        (
            SELECT COUNT(*)
            FROM outcomes o2
            WHERE o2.TEAM_ID = o1.TEAM_ID
            AND o2.SEASON_ID = o1.SEASON_ID
            AND CAST(o2.GAME_DATE AS DATE) 
                BETWEEN (CAST(o1.GAME_DATE AS DATE) - INTERVAL 3 DAY) 
                    AND CAST(o1.GAME_DATE AS DATE)
        ) AS GAMES_LAST_4_DAYS,

        (
            SELECT COUNT(*)
            FROM outcomes o3
            WHERE o3.TEAM_ID = o1.TEAM_ID
            AND o3.SEASON_ID = o1.SEASON_ID
            AND CAST(o3.GAME_DATE AS DATE) 
                BETWEEN (CAST(o1.GAME_DATE AS DATE) - INTERVAL 5 DAY) 
                    AND CAST(o1.GAME_DATE AS DATE)
        ) AS GAMES_LAST_6_DAYS


    FROM outcomes o1
),



record_agg AS (
    SELECT
        TEAM_ID,
        SEASON_ID,
        SEASON_TYPE,
        GAME_ID,
        GAME_DATE,
        SUM(CASE WHEN WL = 'W' THEN 1 ELSE 0 END) OVER (
            PARTITION BY TEAM_ID, SEASON_ID, SEASON_TYPE
            ORDER BY GAME_DATE
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS WINS_SO_FAR,
        SUM(CASE WHEN WL = 'L' THEN 1 ELSE 0 END) OVER (
            PARTITION BY TEAM_ID, SEASON_ID, SEASON_TYPE
            ORDER BY GAME_DATE
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS LOSSES_SO_FAR,
        ROUND(
            SUM(CASE WHEN WL = 'W' THEN 1 ELSE 0 END) OVER (
                PARTITION BY TEAM_ID, SEASON_ID, SEASON_TYPE
                ORDER BY GAME_DATE
                ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
            ) * 1.0
            /
            LEAST(10, ROW_NUMBER() OVER (
                PARTITION BY TEAM_ID, SEASON_ID, SEASON_TYPE
                ORDER BY GAME_DATE
            )),
            3
        ) AS LAST_10_WIN_PCT
    FROM ranked_games
),
-- THIS MAY NEED WORK:: TODO
vs_opponent_record AS (
    SELECT
        r.TEAM_ID,
        r.SEASON_ID,
        r.SEASON_TYPE,
        r.GAME_ID,
        REGEXP_REPLACE(r.MATCHUP, '.*(vs\\.|@) ', '') AS OPP_TEAM_ABBR,
        SUM(CASE WHEN r.WL = 'W' THEN 1 ELSE 0 END) OVER (
            PARTITION BY r.TEAM_ID, OPP_TEAM_ABBR, r.SEASON_ID, r.SEASON_TYPE
            ORDER BY r.GAME_DATE
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS WINS_VS_OPPONENT,
        SUM(CASE WHEN r.WL = 'L' THEN 1 ELSE 0 END) OVER (
            PARTITION BY r.TEAM_ID, OPP_TEAM_ABBR, r.SEASON_ID, r.SEASON_TYPE
            ORDER BY r.GAME_DATE
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS LOSSES_VS_OPPONENT
    FROM ranked_games AS r
)
SELECT
    -- 1. Metadata / Identifiers
    t1.GAME_ID,
    LEAD(t1.game_id) OVER (
      PARTITION BY t1.season_id, t1.team_id
      ORDER BY t1.game_date
    ) AS next_game_id,
    t1.GAME_DATE,
    t1.SEASON_ID,
    t1.SEASON_TYPE,
    t1.TEAM_ID,
    t1.TEAM_ABBREVIATION,
    t1.TEAM_NAME,
    t1.TEAM_CITY,
    t1.HOME_TEAM,
    t1.AWAY_TEAM,
    t1.IS_HOME,
    t1.MATCHUP,
    t1.GAME_NUMBER,
    t1.DAYS_SINCE_LAST_GAME,
    t1.IS_BACK_TO_BACK,
    CASE WHEN t1.GAMES_LAST_4_DAYS >= 3 THEN 1 ELSE 0 END AS IS_3_IN_4,
    CASE WHEN t1.GAMES_LAST_6_DAYS >= 4 THEN 1 ELSE 0 END AS IS_4_IN_6,
    CASE WHEN t1.GAME_DATE = t1.LAST_TEAM_GAME_DATE THEN 1 ELSE 0 END AS IS_LAST_TEAM_GAME,
    t1.LINE,
    t1.OU,

    -- 2. Outcome Stats
    t1.WL,
    t1.SCORE_DIFF,
    t1.OPP_PTS,
    t1.COVER_RESULT,
    t1.OU_RESULT,

    -- 3. Betting Record & Rolling Performance
    r.WINS_SO_FAR,
    r.LOSSES_SO_FAR,
    r.LAST_10_WIN_PCT,
    v.WINS_VS_OPPONENT,
    v.LOSSES_VS_OPPONENT,

    -- 4. Core Box Score Stats
    t1.PTS,
    t1.FGM,
    t1.FGA,
    t1.FG_PCT,
    t1.FG3M,
    t1.FG3A,
    t1.FG3_PCT,
    t1.FTM,
    t1.FTA,
    t1.FT_PCT,
    t1.OREB,
    t1.DREB,
    t1.REB,
    t1.AST,
    t1.STL,
    t1.BLK,
    t1.TOV,
    t1.PF,
    t1.PLUS_MINUS,

    -- 5. Advanced Metrics
    t1.E_OFF_RATING,
    t1.OFF_RATING,
    t1.E_DEF_RATING,
    t1.DEF_RATING,
    t1.E_NET_RATING,
    t1.NET_RATING,
    t1.AST_PCT,
    t1.AST_TOV,
    t1.AST_RATIO,
    t1.OREB_PCT,
    t1.DREB_PCT,
    t1.REB_PCT,
    t1.TM_TOV_PCT,
    t1.EFG_PCT,
    t1.TS_PCT,
    t1.USG_PCT,
    t1.E_USG_PCT,
    t1.E_PACE,
    t1.PACE,
    t1.PACE_PER40,
    t1.POSS,
    t1.PIE,

    -- 6. Miscellaneous Stats
    t1.FTA_RATE,
    t1.OPP_EFG_PCT,
    t1.OPP_FTA_RATE,
    t1.OPP_TOV_PCT,
    t1.OPP_OREB_PCT,
    t1.PTS_OFF_TOV,
    t1.PTS_2ND_CHANCE,
    t1.PTS_FB,
    t1.PTS_PAINT,
    t1.OPP_PTS_OFF_TOV,
    t1.OPP_PTS_2ND_CHANCE,
    t1.OPP_PTS_FB,
    t1.OPP_PTS_PAINT,
    t1.BLKA,
    t1.PFD,

    -- 7. Scoring Breakdown Percentages
    t1.PCT_FGA_2PT,
    t1.PCT_FGA_3PT,
    t1.PCT_PTS_2PT,
    t1.PCT_PTS_2PT_MR,
    t1.PCT_PTS_3PT,
    t1.PCT_PTS_FB,
    t1.PCT_PTS_FT,
    t1.PCT_PTS_OFF_TOV,
    t1.PCT_PTS_PAINT,
    t1.PCT_AST_2PM,
    t1.PCT_UAST_2PM,
    t1.PCT_AST_3PM,
    t1.PCT_UAST_3PM,
    t1.PCT_AST_FGM,
    t1.PCT_UAST_FGM

FROM ranked_games t1
LEFT JOIN record_agg r
    ON t1.TEAM_ID = r.TEAM_ID AND t1.GAME_ID = r.GAME_ID
LEFT JOIN vs_opponent_record v
    ON t1.TEAM_ID = v.TEAM_ID AND t1.GAME_ID = v.GAME_ID;