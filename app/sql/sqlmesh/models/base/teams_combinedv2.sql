MODEL (
    name base.teams_combined,
    kind FULL
);

SELECT DISTINCT
    -- 1. Information / Metadata
    COALESCE(
        CAST(raw.log_table.GAME_ID AS VARCHAR),
        CAST(raw.teams_fourfactors.GAME_ID AS VARCHAR),
        CAST(base.lines_table.GAME_ID AS VARCHAR),
        CAST(raw.teams_advanced.GAME_ID AS VARCHAR),
        CAST(raw.teams_misc.GAME_ID AS VARCHAR),
        CAST(raw.teams_scoring.GAME_ID AS VARCHAR),
        CAST(raw.teams_traditional.GAME_ID AS VARCHAR)
    ) AS GAME_ID,
    COALESCE(raw.log_table.GAME_DATE, base.lines_table.GAME_DATE) AS GAME_DATE,
    COALESCE(
        raw.log_table.TEAM_ABBREVIATION,
        raw.teams_fourfactors.TEAM_ABBREVIATION,
        base.lines_table.TEAM_ABBREVIATION,
        raw.teams_advanced.TEAM_ABBREVIATION,
        raw.teams_misc.TEAM_ABBREVIATION,
        raw.teams_scoring.TEAM_ABBREVIATION,
        raw.teams_traditional.TEAM_ABBREVIATION
    ) AS TEAM_ABBREVIATION,
    raw.log_table.SEASON_ID,
    COALESCE(
        raw.log_table.TEAM_ID,
        raw.teams_fourfactors.TEAM_ID,
        raw.teams_advanced.TEAM_ID,
        raw.teams_misc.TEAM_ID,
        raw.teams_scoring.TEAM_ID,
        raw.teams_traditional.TEAM_ID
    ) AS TEAM_ID,
    COALESCE(
        raw.log_table.TEAM_NAME,
        raw.teams_fourfactors.TEAM_NAME,
        raw.teams_advanced.TEAM_NAME,
        raw.teams_misc.TEAM_NAME,
        raw.teams_scoring.TEAM_NAME,
        raw.teams_traditional.TEAM_NAME
    ) AS TEAM_NAME,
    COALESCE(raw.teams_fourfactors.TEAM_CITY, raw.teams_advanced.TEAM_CITY, raw.teams_misc.TEAM_CITY, raw.teams_scoring.TEAM_CITY, raw.teams_traditional.TEAM_CITY) AS TEAM_CITY,
    raw.log_table.MATCHUP,

    -- 2. Outcome Stats
    raw.log_table.WL,
    COALESCE(raw.log_table.PLUS_MINUS, raw.teams_traditional.PLUS_MINUS) AS PLUS_MINUS,
    base.lines_table.LINE,
    base.lines_table.OU,

    -- 3. Core Box Score Stats (Numeric)
    COALESCE(CAST(raw.log_table.MIN AS VARCHAR),CAST(raw.teams_fourfactors.MIN AS VARCHAR),CAST(raw.teams_advanced.MIN AS VARCHAR),CAST(raw.teams_misc.MIN AS VARCHAR),CAST(raw.teams_scoring.MIN AS VARCHAR),CAST(raw.teams_traditional.MIN AS VARCHAR)) as MIN,
    COALESCE(raw.log_table.PTS, raw.teams_traditional.PTS) AS PTS,
    COALESCE(raw.log_table.FGM, raw.teams_traditional.FGM) AS FGM,
    COALESCE(raw.log_table.FGA, raw.teams_traditional.FGA) AS FGA,
    COALESCE(raw.log_table.FG3M, raw.teams_traditional.FG3M) AS FG3M,
    COALESCE(raw.log_table.FG3A, raw.teams_traditional.FG3A) AS FG3A,
    COALESCE(raw.log_table.FTM, raw.teams_traditional.FTM) AS FTM,
    COALESCE(raw.log_table.FTA, raw.teams_traditional.FTA) AS FTA,
    COALESCE(raw.log_table.OREB, raw.teams_traditional.OREB) AS OREB,
    COALESCE(raw.log_table.DREB, raw.teams_traditional.DREB) AS DREB,
    COALESCE(raw.log_table.REB, raw.teams_traditional.REB) AS REB,
    COALESCE(raw.log_table.AST, raw.teams_traditional.AST) AS AST,
    COALESCE(raw.log_table.STL, raw.teams_traditional.STL) AS STL,
    COALESCE(raw.log_table.BLK, raw.teams_misc.BLK, raw.teams_traditional.BLK) AS BLK,
    raw.log_table.TOV,
    COALESCE(raw.log_table.PF, raw.teams_misc.PF, raw.teams_traditional.PF) AS PF,
    raw.teams_traditional.TO,

    -- 4. Misc Scoring
    raw.teams_misc.PTS_OFF_TOV,
    raw.teams_misc.PTS_2ND_CHANCE,
    raw.teams_misc.PTS_FB,
    raw.teams_misc.PTS_PAINT,
    raw.teams_misc.OPP_PTS_OFF_TOV,
    raw.teams_misc.OPP_PTS_2ND_CHANCE,
    raw.teams_misc.OPP_PTS_FB,
    raw.teams_misc.OPP_PTS_PAINT,
    raw.teams_misc.BLKA,
    raw.teams_misc.PFD,

    -- 5. Percentages / Advanced
    COALESCE(raw.log_table.FG_PCT, raw.teams_traditional.FG_PCT) AS FG_PCT,
    COALESCE(raw.log_table.FG3_PCT, raw.teams_traditional.FG3_PCT) AS FG3_PCT,
    COALESCE(raw.log_table.FT_PCT, raw.teams_traditional.FT_PCT) AS FT_PCT,
    raw.teams_advanced.E_OFF_RATING,
    raw.teams_advanced.OFF_RATING,
    raw.teams_advanced.E_DEF_RATING,
    raw.teams_advanced.DEF_RATING,
    raw.teams_advanced.E_NET_RATING,
    raw.teams_advanced.NET_RATING,
    raw.teams_advanced.AST_PCT,
    raw.teams_advanced.AST_TOV,
    raw.teams_advanced.AST_RATIO,
    COALESCE(raw.teams_fourfactors.OREB_PCT, raw.teams_advanced.OREB_PCT) AS OREB_PCT,
    raw.teams_advanced.DREB_PCT,
    raw.teams_advanced.REB_PCT,
    raw.teams_advanced.E_TM_TOV_PCT,
    COALESCE(raw.teams_fourfactors.TM_TOV_PCT, raw.teams_advanced.TM_TOV_PCT) AS TM_TOV_PCT,
    COALESCE(raw.teams_fourfactors.EFG_PCT, raw.teams_advanced.EFG_PCT) AS EFG_PCT,
    raw.teams_advanced.TS_PCT,
    raw.teams_advanced.USG_PCT,
    raw.teams_advanced.E_USG_PCT,
    raw.teams_advanced.E_PACE,
    raw.teams_advanced.PACE,
    raw.teams_advanced.PACE_PER40,
    raw.teams_advanced.POSS,
    raw.teams_advanced.PIE,
    raw.teams_fourfactors.FTA_RATE,
    raw.teams_fourfactors.OPP_EFG_PCT,
    raw.teams_fourfactors.OPP_FTA_RATE,
    raw.teams_fourfactors.OPP_TOV_PCT,
    raw.teams_fourfactors.OPP_OREB_PCT,

    -- 6. Scoring Percent Breakdown
    raw.teams_scoring.PCT_FGA_2PT,
    raw.teams_scoring.PCT_FGA_3PT,
    raw.teams_scoring.PCT_PTS_2PT,
    raw.teams_scoring.PCT_PTS_2PT_MR,
    raw.teams_scoring.PCT_PTS_3PT,
    raw.teams_scoring.PCT_PTS_FB,
    raw.teams_scoring.PCT_PTS_FT,
    raw.teams_scoring.PCT_PTS_OFF_TOV,
    raw.teams_scoring.PCT_PTS_PAINT,
    raw.teams_scoring.PCT_AST_2PM,
    raw.teams_scoring.PCT_UAST_2PM,
    raw.teams_scoring.PCT_AST_3PM,
    raw.teams_scoring.PCT_UAST_3PM,
    raw.teams_scoring.PCT_AST_FGM,
    raw.teams_scoring.PCT_UAST_FGM,

FROM raw.log_table
LEFT JOIN raw.teams_advanced
    ON raw.log_table.GAME_ID::int = raw.teams_advanced.GAME_ID::int
    AND raw.log_table.TEAM_ABBREVIATION = raw.teams_advanced.TEAM_ABBREVIATION
LEFT JOIN raw.teams_fourfactors
    ON raw.log_table.GAME_ID::int = raw.teams_fourfactors.GAME_ID::int
    AND raw.log_table.TEAM_ABBREVIATION = raw.teams_fourfactors.TEAM_ABBREVIATION
LEFT JOIN raw.teams_traditional
    ON raw.log_table.GAME_ID::int = raw.teams_traditional.GAME_ID::int
    AND raw.log_table.TEAM_ABBREVIATION = raw.teams_traditional.TEAM_ABBREVIATION
LEFT JOIN raw.teams_scoring
    ON raw.log_table.GAME_ID::int = raw.teams_scoring.GAME_ID::int
    AND raw.log_table.TEAM_ABBREVIATION = raw.teams_scoring.TEAM_ABBREVIATION
LEFT JOIN raw.teams_misc
    ON raw.log_table.GAME_ID::int = raw.teams_misc.GAME_ID::int
    AND raw.log_table.TEAM_ABBREVIATION = raw.teams_misc.TEAM_ABBREVIATION
LEFT JOIN base.lines_table
    ON raw.log_table.GAME_ID::int = base.lines_table.GAME_ID::int
    AND raw.log_table.TEAM_ABBREVIATION = base.lines_table.TEAM_ABBREVIATION;
