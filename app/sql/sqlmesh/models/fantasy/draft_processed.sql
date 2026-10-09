MODEL (
  name fantasy.draft_processed,
  kind FULL,
  description """draft data combined with player info for position 
  and adjusted costs"""
);
with season_totals as (
select 
season_id,
sum(cost) / 10 as wallet_size
from fantasy.auction_draft
group by season_id
)
,draft_data as (
select
t1.season_id,
t1.fantasy_team,
t2.player_id,
t1.player_name,
t1.cost,
t1.cost / t3.wallet_size as adjusted_cost
from
fantasy.auction_draft t1
left join (select distinct player_name, player_id from base.players_combined) as t2
ON REPLACE(t1.player_name, '.', '') = REPLACE(t2.player_name, '.', '')
left join season_totals t3 on t1.season_id = t3.season_id
)

select
t1.season_id as DRAFT_SEASON,
t1.adjusted_cost,
t3.POSITION,
t2.* EXCLUDE (SEASON_ID, GAME_DATE)

from draft_data t1
join fantasy.z_scores t2
on t1.player_id = t2.player_id and t1.season_id-1 = t2.season_id
join raw.common_player_info t3
on t2.player_id = t3.person_id