
WITH deduped_game_details as (

select  gd.*,
		g.game_date_est,
		g.season,
		g.home_team_id,
		row_number() over (partition by gd.game_id, team_id, player_id order by game_date_est) as row_num
	from game_details gd inner join games g on gd.game_id=g.game_id
	
)
INSERT INTO fact_game_details
select 
	game_date_est as dim_game_date,
	season as dim_season,
	team_id as dim_team_id,
	player_id dim_player_id,
	player_name dim_player_name,
	start_position dim_start_position,
	team_id=home_team_id as dim_is_playing_at_home,
	COALESCE(POSITION('DNP' in comment),0)>0 as dim_did_not_play,
	COALESCE(POSITION('DND' in comment),0)>0 as dim_did_not_dressed,
	COALESCE(POSITION('NWT' in comment),0)>0 as dim_not_with_team,
	CAST(SPLIT_PART(min,':',1) AS REAL) + CAST(SPLIT_PART(min,':',2) AS REAL)/60 AS minutes,
	fgm as m_fgm,
	fga as m_fga,
	fg3m as m_fg3m,
	fg3a as m_fg3a,
	ftm as m_ftm,
	fta as m_fta,
	oreb as m_oreb,
	dreb as m_dreb,
	reb as m_reb,
	ast as m_ast,
	stl as m_stl,
	blk as m_blk,
	"TO" as m_turnovers,
	pf as m_pf,
	pts as m_pts,
	plus_minus as m_plus_minus
from deduped_game_details
where row_num=1;
-- CREATE TABLE fact_game_details (

-- 	dim_game_date DATE,
-- 	dim_season INTEGER,
-- 	dim_team_id INTEGER,
-- 	dim_player_id INTEGER,
-- 	dim_player_name TEXT,
-- 	dim_start_position TEXT,
-- 	dim_is_playing_at_home BOOLEAN,
-- 	dim_did_not_play BOOLEAN,
-- 	dim_did_not_dresses BOOLEAN,
-- 	dim_not_with_team BOOLEAN,
-- 	m_minutes REAL,
-- 	m_fgm REAL,
-- 	m_fga REAL,
-- 	m_fg3m REAL,
-- 	m_fg3a REAL,
-- 	m_ftm REAL,
-- 	m_fta REAL,
-- 	m_oreb REAL,
-- 	m_dreb REAL,
-- 	m_reb REAL,
-- 	m_ast REAL,
-- 	m_stl REAL,
-- 	m_blk REAL,
-- 	m_turnovers REAL,
-- 	m_pf REAL,
-- 	m_pts REAL,
-- 	m_plus_minus  REAL,
-- 	PRIMARY KEY (dim_game_date,dim_team_id,dim_player_id))

select * from fact_game_details