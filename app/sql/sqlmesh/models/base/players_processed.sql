MODEL (
    name base.players_processed,
    kind FULL
);
WITH MINUTE_TABLE AS (
  SELECT *,
  CASE WHEN MIN IS NULL THEN '00:00'
  WHEN MIN = '' THEN '00:00'
  else MIN end as MINUTES
  FROM base.players_combined 
)
WITH player_games AS (
  SELECT 
    *,
CASE 
  WHEN
       (
         TRY_CAST(SPLIT_PART(MINUTES, ':', 1) AS DOUBLE) > 0 
         OR TRY_CAST(SPLIT_PART(MINUTES, ':', 2) AS DOUBLE) > 0
       )
  THEN 1
  ELSE 0
END AS PLAYED_FLAG,
    LEAD(game_id) OVER (
      PARTITION BY player_id, season_id
      ORDER BY game_date
    ) AS next_game_id

  FROM MINUTE_TABLE

),

ranked_games AS (
  SELECT *,
  
    -- Sequential game count (includes DNPs)
    ROW_NUMBER() OVER (
      PARTITION BY player_id, season_id 
      ORDER BY game_date
    ) AS game_count,

    -- Cumulative count of games played
    SUM(
      CASE 
        WHEN PLAYED_FLAG = 1 THEN 1 
        ELSE 0 
      END
    ) OVER (
      PARTITION BY player_id, season_id
      ORDER BY game_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS games_played,

    -- Row number for most recent *played* game
    ROW_NUMBER() OVER (
      PARTITION BY player_id, season_id
      ORDER BY CASE WHEN PLAYED_FLAG = 1 THEN game_date ELSE NULL END DESC
    ) AS rn_played

  FROM player_games
)



SELECT

season_id,
team_id,
team_abbreviation,
team_name,
game_id,
NEXT_GAME_ID,
game_date,
team_city,
player_id,
player_name,
nickname,
start_position,
comment,
GAME_COUNT,
PLAYED_FLAG,
GAMES_PLAYED,
CASE 
  WHEN rn_played = 1 AND played_flag = 1 THEN 1 
  ELSE 0 
END AS is_last_game,
matchup,
wl,
plus_minus,
COALESCE(
  TRY_CAST(SPLIT_PART(MINUTES, ':', 1) AS DOUBLE) 
  + TRY_CAST(SPLIT_PART(MINUTES, ':', 2) AS DOUBLE) / 60,
  0
) AS MINUTES,
pts,
fgm,
fga,
fg_pct,
fg3m,
fg3a,
fg3_pct,
ftm,
fta,
ft_pct,
oreb,
dreb,
reb,
ast,
stl,
blk,
pf,
"to",
e_off_rating,
off_rating,
e_def_rating,
def_rating,
e_net_rating,
net_rating,
ast_pct,
ast_tov,
ast_ratio,
oreb_pct,
dreb_pct,
reb_pct,
tm_tov_pct,
efg_pct,
ts_pct,
usg_pct,
e_usg_pct,
e_pace,
pace,
pace_per40,
poss,
pie,
fta_rate,
opp_efg_pct,
opp_fta_rate,
opp_tov_pct,
opp_oreb_pct,
pts_off_tov,
pts_2nd_chance,
pts_fb,
pts_paint,
opp_pts_off_tov,
opp_pts_2nd_chance,
opp_pts_fb,
opp_pts_paint,
blka,
pfd,
pct_fga_2pt,
pct_fga_3pt,
pct_pts_2pt,
pct_pts_2pt_mr,
pct_pts_3pt,
pct_pts_fb,
pct_pts_ft,
pct_pts_off_tov,
pct_pts_paint,
pct_ast_2pm,
pct_uast_2pm,
pct_ast_3pm,
pct_uast_3pm,
pct_ast_fgm,
pct_uast_fgm

FROM ranked_games
WHERE PLAYER_ID is not null;
