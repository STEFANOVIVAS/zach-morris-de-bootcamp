-- A query to deduplicate `game_details` from Day 1 so there's no duplicates

select * from
(select *,row_number() over (partition by game_id,team_id,player_id) as row_num
from game_details) A
where row_num=1