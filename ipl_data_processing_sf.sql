-- set some basic requirements for the project --

set ipl_schema = 'IPL_SCHEMA';
set ipl_warehouse = 'IPL_WAREHOUSE';

CREATE USER new_user
PASSWORD = 'Reddy@28'
DEFAULT_ROLE = 'PUBLIC' -- You can specify a default role if needed
MUST_CHANGE_PASSWORD = FALSE; -- Set to TRUE if you want the user to change their password upon first login


SHOW USERS;

-- create a warehouse for the project with size as xsmall --

-- I suggest you to see the data in the json graph usig the json viewer in the web to get a better idea about the json data --

create warehouse if not exists identifier($ipl_warehouse)
warehouse_size = xsmall
warehouse_type = standard
auto_suspend = 60
auto_resume =true
initially_suspended = true;

--create a database for the project to store all the objects --

create database if not exists identifier($ipl_database);

use database identifier($ipl_database);

-- using the the same database create the different schemas for the  project --

create or replace schema land;
create or replace schema raw;
create or replace schema clean;
create or replace schema consumption;

-- In the land schema we load the all the data into the stage with the specific format --
use schema land;

-- As the data we have downloaded is of the form json we created the json format --
create or replace file format land.my_json_format
type = 'json'
null_if = ('I\n', 'null','')
strip_outer_array = true
comment = 'Json File Format with outer stip array flag true';

-- We are creating the stage to store the json raw data --
create or replace stage land.my_stg;

-- we are just checking for any previous data exist or not --

list @land.my_stg/cricket/json;

-- I have completely pulled the json files into the stage using the terminal connection --

use schema land;

-- now have just checking the data in the particular json file how it looks --
select
t. $1:meta:: variant as meta,
t. $1: info:: variant as info,
t. $1:innings::array as innings,
metadata$filename as file_name, 
metadata$file_row_number int, 
metadata$file_content_key text, 
metadata$file_last_modified stg_modified_ts
from
@land.my_stg/cricket/json/335982.json (file_format => 'my_json_format') t;

-- Now we are using the other schema named raw to store all the raw data from different json files into single table --
use schema raw;

-- I have created the raw table which stores the meta data, info data and innings data from the all json tables--
create or replace transient table ipl_database.raw.match_raw_tbl (
meta object not null, 
info variant not null, 
innings ARRAY not null, 
stg_file_name text not null, 
stg_file_row_number int not null, 
stg_file_hashkey text not null, 
stg_modified_ts timestamp not null)
comment = 'This is raw table to store all the json data file with root elements extracted'
;

-- Here i have just selected the all the raw data from the json files and pushed into the raw table in raw schema--
copy into ipl_database.raw.match_raw_tbl from 
(
select
t.$1 :meta:: object as meta,
t.$1: info:: variant as info,
t.$1: innings:: array as innings,
metadata$filename, 
metadata$file_row_number, 
metadata$file_content_key,
metadata$file_last_modified
from @ipl_database.land.my_stg/cricket/json (file_format => 'ipl_database.land.my_json_format')  t)
on_error = continue;


-- I am checking the total rows of the table -- 
select count(*) from ipl_database.raw.match_raw_tbl; --1095 rows


select
meta['data_version']:: text as data_version,
meta[ 'created']:: date as created,
meta['revision']:: number as revision,
from
ipl_database.raw.match_raw_tbl;

-- We have completely moved the raw data from the land schema to the raw schema --

-- Now we need to clean the raw data , so I am doing the cleaning process in the clean schema --

use database ipl_database;
use schema clean;

-- Here in first table i am stroing the all the meta details about the match --

create or replace TABLE IPL_DATABASE.CLEAN.MATCH_DETAILS_CLEAN (
    MATCH_NUMBER INT PRIMARY KEY,
	MATCH_TYPE VARCHAR(16777216),
	SEASON_MATCH_NUMBER NUMBER(38,0),
    PLAY_OFFS VARCHAR(16777216),
	SEASON VARCHAR(16777216),
	TEAM_TYPE VARCHAR(16777216),
	OVERS NUMBER(38,0),
	CITY VARCHAR(16777216),
	VENUE VARCHAR(16777216),
    MATCH_REFREE VARCHAR(16777216),
	TV_UMPIRE VARCHAR(16777216),
	UMPIRE1 VARCHAR(16777216),
	UMPIRE2 VARCHAR(16777216),
	DATE DATE,
	GENDER VARCHAR(16777216),
	FIRST_TEAM VARCHAR(16777216),
	SECOND_TEAM VARCHAR(16777216),
	MATACH_RESULT VARCHAR(16777216),
	WINNER VARCHAR(16777216),
	TOSS_WINNER VARCHAR(16777216),
	TOSS_DECISION VARCHAR(16777216),
	STG_FILE_NAME VARCHAR(16777216),
	STG_FILE_ROW_NUMBER NUMBER(38,0),
	STG_FILE_HASHKEY VARCHAR(16777216),
	STG_MODIFIED_TS TIMESTAMP_NTZ(9)
);

-- I have extracted the all the details related to the meta data from the raw table into the match details table in the clean schema --

create or  replace table ipl_database.clean.match_details_clean1 as
(with cte1 as (select
info:match_type:: text as match_type,
info:event.match_number:: text as season_match_number,
info:season:: text as season,
info:team_type:: text as team_type, 
info:overs:: int as overs,
info:city:: text as city,
REGEXP_REPLACE(info: venue,'["\\[\\]]',''):: text as venue,
REGEXP_REPLACE(info: officials.match_referees,'["\\[\\]]', ''):: text as match_refree,
REGEXP_REPLACE(info: officials.tv_umpires,'["\\[\\]]', ''):: text as tv_umpire,
REGEXP_REPLACE(info: officials.umpires[0],'["\\[\\]]', ''):: text as umpire1,
REGEXP_REPLACE(info: officials.umpires[1],'["\\[\\]]', ''):: text as umpire2,
SUBSTRING(REGEXP_REPLACE(info:dates, '["\\[\\]]', ''),0,10)::date as date,
info: gender:: text as gender, 
info: teams [0]:: text as first_team, 
info: teams [1]:: text as second_team,
case when info:outcome.winner is not null then 'Result Declared'
when info:outcome.result = 'tie' then 'Tie'
when info:outcome.result = 'no result' then 'No Result'
else info:outcome.result
end as matach_result, case when info:outcome.winner is not null then info:outcome.winner
else 'NA'
end as winner,
info: toss.winner::text as toss_winner, 
initcap(info:toss.decision:: text) as toss_decision,
stg_file_name,
stg_file_row_number,
stg_file_hashkey,
stg_modified_ts
from ipl_database.raw.match_raw_tbl)
select row_number() over(order by date,season_match_number asc) match_number, * from cte1
where season = '2012' and season_match_number = 32);


-- I wanted to add the new column for the existing table to show weather it is a playoff or not --

alter table ipl_database.clean.match_details_clean1 ADD COLUMN play_offs text;


-- I have been imputing the values based on the match date and the season match number --
-- If the season has only 3 playoffs then we have to impute it correctly giving the null value based on the date as seminfinal_1 , semifinal_2 , final respectively. I have season that there are only 3 playoffs for the seasons 2008,2009.--
-- Also if season has four playoff matches we have to impute the values with the qualifer_1, eliminator qualifier_2 and fial in a respective manner -- 
-- If they are league matches we have to insert NA as the value--

update ipl_database.clean.match_details_clean1
set play_offs = case
when match_number in 
(select match_number from(select * , rank() over(partition by season order by match_number) as rnk 
from ipl_database.clean.match_details_clean1 
where season_match_number is null and season in('2007/08','2009'))
where rnk = 1)
then 'SemiFinal_1'
when match_number in 
(select match_number from(select * , rank() over(partition by season order by match_number) as rnk 
from ipl_database.clean.match_details_clean1 
where season_match_number is null and season in('2007/08','2009'))
where rnk = 2)
then 'SemiFinal_2'
when match_number in 
(select match_number from(select * , rank() over(partition by season order by match_number) as rnk 
from ipl_database.clean.match_details_clean1 
where season_match_number is null and season in('2007/08','2009'))
where rnk = 3)
then 'Final'
when match_number in 
(select match_number from(select * , rank() over(partition by season order by match_number) as rnk 
from ipl_database.clean.match_details_clean1 
where season_match_number is null and season not in ('2007/08','2009'))
where rnk = 1)
then 'Qualifier_1'
when match_number in 
(select match_number from(select * , rank() over(partition by season order by match_number) as rnk 
from ipl_database.clean.match_details_clean1 
where season_match_number is null and season not in ('2007/08','2009'))
where rnk = 2)
then 'Eliminator'
when match_number in 
(select match_number from(select * , rank() over(partition by season order by match_number) as rnk 
from ipl_database.clean.match_details_clean1 
where season_match_number is null and season not in ('2007/08','2009'))
where rnk = 3)
then 'Qualifier_2'
when match_number in 
(select match_number from(select * , rank() over(partition by season order by match_number) as rnk 
from ipl_database.clean.match_details_clean1 
where season_match_number is null and season not in ('2007/08','2009'))
where rnk = 4)
then 'Final'
else 'NA'
end;

-- First I have created a temporary table to store the data and finally I am moving the data into the my original table --
-- It would be a good practice to create a table and insert values into it instead of creating it directly from select statement --

INSERT INTO ipl_database.clean.match_details_clean (MATCH_NUMBER, MATCH_TYPE, SEASON_MATCH_NUMBER,play_offs, SEASON, TEAM_TYPE, OVERS, CITY, VENUE,MATCH_REFREE,TV_UMPIRE,UMPIRE1,UMPIRE2, DATE,  GENDER, FIRST_TEAM, SECOND_TEAM,MATACH_RESULT, WINNER, TOSS_WINNER, TOSS_DECISION, STG_FILE_NAME, STG_FILE_ROW_NUMBER, STG_FILE_HASHKEY,STG_MODIFIED_TS)
SELECT MATCH_NUMBER, MATCH_TYPE, SEASON_MATCH_NUMBER,play_offs,SEASON, TEAM_TYPE, OVERS, CITY,  VENUE,MATCH_REFREE,TV_UMPIRE,UMPIRE1,UMPIRE2, DATE , GENDER, FIRST_TEAM, SECOND_TEAM,MATACH_RESULT,WINNER, TOSS_WINNER, TOSS_DECISION, STG_FILE_NAME, STG_FILE_ROW_NUMBER, STG_FILE_HASHKEY,STG_MODIFIED_TS
FROM ipl_database.clean.match_details_clean1;

-- As I have the complete data in my main table , I am deleting the temporary table as it is unuseful and it also costs the credits in the snowflake --
-- So it is a better practice to drop the objects which ever you fell not useful anymore in the snowflake --

DROP TABLE ipl_database.clean.match_details_clean1;

select * from ipl_database.clean.match_details_clean
order by match_number;--1095
-- Previously we have seen in raw schema that we have totally 1095 rows and here also we have seen 1095 rows , so may confirm that all the rows have inserted perfectly into the match_details table --

-- So we have completly stored the meta data about all the matches --


-- Now I wanted to extract the players of the every match and need to store in the players table --

create or replace table ipl_database.clean.player_clean_tbl1 as(
with cte1 as (select
SUBSTRING(REGEXP_REPLACE(info:dates, '["\\[\\]]', ''),0,10)::date as date,
info:event.match_number:: int as season_match_number,
raw.info:players as players, 
SUBSTRING(info:season,0,4):: text as season,
raw.info:teams as team,stg_file_name,
stg_file_row_number,
stg_file_hashkey,
stg_modified_ts
from ipl_database.raw.match_raw_tbl raw),
cte2 as (select row_number() over(order by date,season_match_number asc) as match_number , * from cte1)
select
match_number,
season,
p.key:: text as team,
REGEXP_REPLACE(player.value,'["\\[\\]]', '') as player_name,
stg_file_name,
stg_file_row_number,
stg_file_hashkey,
stg_modified_ts
from cte2,
lateral flatten (input => players) p,
lateral flatten (input => p.value) player);

-- I have created a temporary table which stores inforamtion about all the players of the each match in the players table --

-- To store the data into our main table,firstly I hav created the table with basic columns names --

create or replace TABLE IPL_DATABASE.CLEAN.PLAYER_CLEAN_TBL(
    ID INT PRIMARY KEY IDENTITY(1,1) ,
	MATCH_NUMBER INT,
    SEASON INT,
	TEAM VARCHAR(16777216),
	PLAYER_NAME VARCHAR(16777216),
	STG_FILE_NAME VARCHAR(16777216),
	STG_FILE_ROW_NUMBER NUMBER(38,0),
	STG_FILE_HASHKEY VARCHAR(16777216),
	STG_MODIFIED_TS TIMESTAMP_NTZ(9)
);

-- Now I am inserting the all the data about the players froom temprary table to our main table , as I have previously metioned we can even create table directly by making the temorary table as main table, but I always do creating the table with required columns and insert the values into it--

-- Now I have inserted the all players information into the players table --
insert into IPL_DATABASE.CLEAN.PLAYER_CLEAN_TBL(MATCH_NUMBER,SEASON,TEAM,PLAYER_NAME,STG_FILE_NAME,STG_FILE_ROW_NUMBER,STG_FILE_HASHKEY,STG_MODIFIED_TS)
select MATCH_NUMBER,SEASON,TEAM,PLAYER_NAME,STG_FILE_NAME,STG_FILE_ROW_NUMBER,STG_FILE_HASHKEY,STG_MODIFIED_TS FROM IPL_DATABASE.CLEAN.PLAYER_CLEAN_TBL1 ;

-- So I just droping the table that is not required anymore as it costs the credits --

drop table IPL_DATABASE.CLEAN.player_clean_tbl1;

select count(*) from ipl_database.clean.player_clean_tbl;--24367

-- We have 1095 matches and in each match totally around 22 palyers will be playing so the row count is 24367--

-- I have checking for the details about the players table --
desc table ipl_database.clean.match_details_clean; 

-- I have altered the table where some columns not to be null--
alter table ipl_database.clean.player_clean_tbl modify column match_number set not null;
alter table ipl_database.clean.player_clean_tbl modify column season set not null;
alter table ipl_database.clean.player_clean_tbl modify column team set not null;
alter table ipl_database.clean.player_clean_tbl modify column player_name set not null;

-- You know we have to link the players table with the match details table, for that I have selected the primary key of match table and linked it to the match_number as the  foreign key for the players table --

alter table ipl_database.clean.player_clean_tbl
add constraint fk_match_id foreign key (match_number)
references ipl_database.clean.match_details_clean (match_number);


with cte1 as(
select
SUBSTRING(REGEXP_REPLACE(info:dates, '["\\[\\]]', ''),0,10)::date as date,
info:event.match_number:: int as season_match_number,
m. innings as innings  from ipl_database.raw.match_raw_tbl m), 
cte2 as (select row_number() over(order by date,season_match_number) as match_number, innings from cte1)
select * from cte2
where match_number =4;

-- Now as the part of cleaing the data we have one step yet to be done , to store the details about the deliveries of the every match --
-- I have found json viewer that delivery details are stored compeltly in the innings extention --

-- I have extracted the all the details about the delivery details into one temporary table --

create or replace transient table ipl_database.clean.delivery_clean_tbl1 as
with cte1 as(
select
SUBSTRING(REGEXP_REPLACE(info:dates, '["\\[\\]]', ''),0,10)::date as date,
SUBSTRING(info:season,0,4):: text as season,
info:event.match_number:: int as season_match_number,
m. innings as innings,
m.stg_file_name as stg_file_name,
m.stg_file_row_number as stg_file_row_number,
m.stg_file_hashkey as stg_file_hashkey,
m.stg_modified_ts as stg_modified_ts
from ipl_database.raw.match_raw_tbl m), 
cte2 as (select row_number() over(order by date,season_match_number) as match_number, innings,
season,
stg_file_name,
stg_file_row_number,
stg_file_hashkey,
stg_modified_ts
from cte1)
select 
match_number :: int as match_id,
season,
i. value:team::text as team_name,
o. value:over:: int as over,
d. value:bowler:: text as bowler,
d. value:batter:: text as batter,
d. value:non_striker::text as non_striker,
d. value:runs.batter:: text as runs,
d. value:runs.extras:: text as extras,
d. value:runs.total: text as total,
e. key:: text as extra_type,
e. value:: number as extra_runs,
w. value:player_out::text as player_out,
w. value:kind:: text as player_out_kind,
w. value:fielders::variant as player_out_fielders,
stg_file_name,
stg_file_row_number,
stg_file_hashkey,
stg_modified_ts
from cte2,
lateral flatten (input => innings) i,
lateral flatten (input => i.value:overs) o,
lateral flatten (input => o.value:deliveries) d,
lateral flatten (input => d.value:extras,outer => True) e,
lateral flatten (input => d.value:wickets,outer => True) w;


-- Now I have created a table manually with the all the required fields for the delivery table --

create or replace TRANSIENT TABLE IPL_DATABASE.CLEAN.DELIVERY_CLEAN_TBL (
    DELIVERY_ID INT PRIMARY KEY IDENTITY(1,1),
	MATCH_ID NUMBER(38,0) NOT NULL,
    SEASON INT ,
	TEAM_NAME VARCHAR(16777216) NOT NULL,
	OVER NUMBER(38,0) NOT NULL,
	BOWLER VARCHAR(16777216) NOT NULL,
	BATTER VARCHAR(16777216) NOT NULL,
	NON_STRIKER VARCHAR(16777216) NOT NULL,
	RUNS VARCHAR(16777216),
	EXTRAS VARCHAR(16777216),
	TOTAL VARIANT,
	EXTRA_TYPE VARCHAR(16777216),
	EXTRA_RUNS NUMBER(38,0),
	PLAYER_OUT VARCHAR(16777216),
	PLAYER_OUT_KIND VARCHAR(16777216),
	PLAYER_OUT_FIELDERS VARIANT,
	STG_FILE_NAME VARCHAR(16777216),
	STG_FILE_ROW_NUMBER NUMBER(38,0),
	STG_FILE_HASHKEY VARCHAR(16777216),
	STG_MODIFIED_TS TIMESTAMP_NTZ(9)
);

--As part of our regular process , I have inserted all the details into the main table from the temporary table --

INSERT INTO IPL_DATABASE.CLEAN.DELIVERY_CLEAN_TBL(MATCH_ID,SEASON,TEAM_NAME,OVER,BOWLER,BATTER,NON_STRIKER,RUNS,EXTRAS,TOTAL,EXTRA_TYPE,EXTRA_RUNS,PLAYER_OUT,PLAYER_OUT_KIND,PLAYER_OUT_FIELDERS,STG_FILE_NAME,STG_FILE_ROW_NUMBER,STG_FILE_HASHKEY,STG_MODIFIED_TS)
SELECT MATCH_ID,SEASON,TEAM_NAME,OVER,BOWLER,BATTER,NON_STRIKER,RUNS,EXTRAS,TOTAL,EXTRA_TYPE,EXTRA_RUNS,PLAYER_OUT,PLAYER_OUT_KIND,PLAYER_OUT_FIELDERS,STG_FILE_NAME,STG_FILE_ROW_NUMBER,STG_FILE_HASHKEY,STG_MODIFIED_TS
from ipl_database.clean.delivery_clean_tbl1;

--Now have droped the table which is not needed anymore --

drop table ipl_database.clean.delivery_clean_tbl1;


select count(*) from ipl_database.clean.delivery_clean_tbl;--260945

-- We know that for each ipl match there will be 40 overs, so we have 1095 matches (1095*40*6) = 262800 which is almost near to 260945 , it is because sometimes matches will finish within less than 20 overs --

desc table ipl_database.clean.delivery_clean_tbl;

-- Now have changing the table where some columns need to be not null--

alter table ipl_database.clean.delivery_clean_tbl modify column match_id set not null;
alter table ipl_database.clean.delivery_clean_tbl modify column season set not null;
alter table ipl_database.clean.delivery_clean_tbl modify column team_name set not null;
alter table ipl_database.clean.delivery_clean_tbl modify column over set not null;
alter table ipl_database.clean.delivery_clean_tbl modify column bowler set not null;
alter table ipl_database.clean.delivery_clean_tbl modify column batter set not null;
alter table ipl_database.clean.delivery_clean_tbl modify column non_striker set not null;

--Finally I am linking the ddelivery table to the match_details table--
alter table ipl_database.clean.delivery_clean_tbl add constraint fk_delivery_match_id foreign key (match_id) references ipl_database.clean.match_details_clean (match_number);


-- We have successfully completed the process of extracting the data into structured format from the raw json data --



select
team_name, batter, sum (runs)
from
delivery_clean_tbl
where match_number = 9
group by team_name, batter order by 3 desc;

select
team_name,
sum (runs) + sum (extra_runs) as total_runs
from
delivery_clean_tbl
where match_number=12
group by team_name order by 2 desc;



-- Now the main part comes where we have devided the data into different tables and distinction from the fact and dimension tables --

-- For that I have using the other schema called consumption where we get the final product in the structured format --

use role accountadmin;
use warehouse ipl_warehouse;
use schema ipl_database.consumption;


-- Firsty I create the date dimension table to store the data about the dates of the matches played --

create or replace table date_dim (
date_id int primary key autoincrement, 
full_dt date, 
day int, 
month int, 
year int, 
quarter int, 
dayofweek int,
dayofmonth int,
dayofyear int,
dayofweekname varchar (3), -- to store day names (e.g., "Mon")
isweekend boolean -- to indicate if it's a weekend (True/False Sat/Sun both falls underweekend)
) ;


-- Refree_dim stores the referes for the every match --
create or replace table referee_dim(
referee_id int primary key autoincrement, 
refere text not null
);

-- I have found that there are some null values for the tv_umpire in the main table from the clean schema , so I just imputing the nul values with 'NA';

update ipl_database.clean.match_details_clean
set tv_umpire = 'NA'
where tv_umpire is null;

-- Tv_umpire_dim stores data about the tv umpirees for the each match --
create or replace table tv_umpire_dim(
tv_umpire_id int primary key autoincrement, 
tv_umpire text
);

-- umpires_dim stores the on field umpires details --
create or replace table umpires_dim(
umpire_id int primary key autoincrement, 
umpire text not null
);

-- team_dim stores the names of the different teams that have been played so far in the ipl history --
create or replace table team_dim (
team_id int primary key autoincrement, 
team_name text not null
);

--season_dim is to store the names of the season --
create or replace table season_dim (
season_id int primary key autoincrement, 
season text not null
);

--Here we are stroing the details of the player and the teams that he had played i his ipl carrer --
create or replace table player_dim (
player_id int primary key autoincrement, 
player_name text not null,
team_name text not null,
season_name text not null
);

-- This stores the details about the stadium the match held --
create or replace table venue_dim (
venue_id int primary key autoincrement, 
venue_name text not null, 
city text not null, 
state text, 
country text, 
continent text, 
end_names text, 
capacity number, 
pitch text, 
flood_light boolean, 
established_dt date, 
playing_area text, 
other_sports text, 
curator text,
lattitude number (10,6) ,
longitude number (10,6)
);


-- It is just for our understanding as the match type is t20 --
create or replace table match_type_dim (
match_type_id int primary key autoincrement, 
match_type text not null
);


--Play_off_dim table is to know which match they are playing like league, semifinals or final --
create or replace table play_off_dim (
play_off_id int primary key autoincrement, 
play_offs text 
);

-- match_fact table is the fact table which will be having almost the numeric values and references from the dimension tables --

CREATE or replace TABLE match_fact (
match_id INT PRIMARY KEY,
season_id INT NOT NULL,
season_match NUMBER(3),
play_off_id INT NOT NULL, 
date_id INT NOT NULL,
referee_id INT NOT NULL,
tv_umpire_id INT NOT NULL,
umpire_id1 INT NOT NULL,
umpire_id2 INT NOT NULL,
team_a_id INT NOT NULL,
team_b_id INT NOT NULL,
venue_id INT NOT NULL,
match_type_id INT NOT NULL,
total_overs number (3),
balls_per_over number (1),

overs_played_by_team_a number(2),
bowls_played_by_team_a number(3) ,
extra_bowls_played_by_team_a number(3),
extra_runs_scored_by_team_a number(3),
fours_by_team_a number(3),
sixes_by_team_a number(3),
total_score_by_team_a number (3),
wicket_lost_by_team_a number (2),

overs_played_by_team_b number(2), 
bowls_played_by_team_b number(3), 
extra_bowls_played_by_team_b number(3) , 
extra_runs_scored_by_team_b number(3),
fours_by_team_b number(3), 
sixes_by_team_b number(3), 
total_score_by_team_b number(3), 
wicket_lost_by_team_b number(2),

toss_winner_team_id int, 
toss_decision text, 
match_result text not null, 
winner_team_id int,

CONSTRAINT fk_date FOREIGN KEY (date_id) REFERENCES date_dim (date_id),
CONSTRAINT fk_season FOREIGN KEY (season_id) REFERENCES season_dim(season_id),
CONSTRAINT fk_play_off FOREIGN KEY (play_off_id) REFERENCES play_off_dim(play_off_id),
CONSTRAINT k_referee FOREIGN KEY (referee_id) REFERENCES referee_dim (referee_id),
CONSTRAINT k_tv_umpire FOREIGN KEY (tv_umpire_id) REFERENCES tv_umpire_dim (tv_umpire_id),
CONSTRAINT k_umpire FOREIGN KEY (umpire_id1) REFERENCES umpires_dim(umpire_id),
CONSTRAINT k1_umpire FOREIGN KEY (umpire_id2) REFERENCES umpires_dim(umpire_id),
CONSTRAINT fk_team1 FOREIGN KEY (team_a_id) REFERENCES team_dim (team_id),
CONSTRAINT fk_team2 FOREIGN KEY (team_b_id) REFERENCES team_dim (team_id),
CONSTRAINT fk_venue FOREIGN KEY (venue_id) REFERENCES venue_dim (venue_id),
CONSTRAINT fk_match_type FOREIGN KEY (match_type_id) REFERENCES match_type_dim (match_type_id),
CONSTRAINT fk_toss_winner_team FOREIGN KEY (toss_winner_team_id) REFERENCES team_dim (team_id),
CONSTRAINT fk_winner_team FOREIGN KEY (winner_team_id) REFERENCES team_dim (team_id)
);



-- Now I start inserting into the each dimension and fact table --

--Make sure to insert the distinct values into the dimension tables, there should be any duplicates --

-- I have inserted the team names into the team_dim --
insert into ipl_database.consumption.team_dim (team_name)
select distinct team_name from (
select first_team as team_name from ipl_database.clean.match_details_clean 
union all
select second_team as team_name from ipl_database.clean.match_details_clean
)order by team_name;

insert into ipl_database.consumption.team_dim(team_name) values ('NA');


-- Inserted the season names into the season_dim table --

insert into ipl_database.consumption.season_dim(season)
select 
season 
from ipl_database.clean.match_details_clean
group by season
order by season;

select * from ipl_database.consumption.team_dim;

--Now I inserted the data into the player_dim table with team they have played, along with the season they played --
-- Here there is chance that one player can play for different team in different teams , so it is combination of the  player, team , season--

insert into ipl_database.consumption.player_dim (player_name,team_name,season_name)
select player_name,team,season
from ipl_database.clean.player_clean_tbl
group by
player_name,
team,
season
order by player_name;

--totally there is 1443 distinct combinations among the player, team and season;

select count(1) from ipl_database.consumption.player_dim;--1443

--into the refree_dim , umpire_dim , tv_umpire_dim tables inserted the  values with the distinct values--
insert into ipl_database.consumption.referee_dim(refere)
select
MATCH_REFREE
from
ipl_database.clean.match_details_clean
group by match_refree;


insert into ipl_database.consumption.tv_umpire_dim(tv_umpire)
select
distinct tv_umpire
from
ipl_database.clean.match_details_clean
group by tv_umpire;

insert into ipl_database.consumption.umpires_dim(umpire)
select
distinct umpire1
from
ipl_database.clean.match_details_clean
union
select 
distinct umpire2
from 
ipl_database.clean.match_details_clean;
select * from ipl_database.consumption.umpires_dim
order by umpire;

-- Here I found that some changes need to be done for the venue details --
select venue, city from ipl_database.clean.match_details_clean;
-- v3 
select
venue, city
from
ipl_database.clean.match_details_clean
group by
venue, city;

-- In the venue name in match_details table I have found that city name has been included so to correct that I splited the row based on the delimiter ;
UPDATE ipl_database.clean.match_details_clean
SET venue = SPLIT_PART(venue, ',', 1);

-- And also I found some duplicate names been included , so updated the table with  all changes --
UPDATE ipl_database.clean.match_details_clean
SET venue = 'M Chinnaswamy Stadium'
where venue = 'M.Chinnaswamy Stadium';

UPDATE ipl_database.clean.match_details_clean
SET venue = 'Punjab Cricket Association Stadium'
where venue = 'Punjab Cricket Association IS Bindra Stadium';

update ipl_database.clean.match_details_clean
set city = 'Bengaluru'
where city = 'Bangalore';

update ipl_database.clean.match_details_clean
set venue = TRIM(venue);

update ipl_database.clean.match_details_clean
set city = TRIM(city);

update ipl_database.clean.match_details_clean
set city = 'Navi Mumbai'
where city = 'Mumbai' and venue = 'Dr DY Patil Sports Academy';

update ipl_database.clean.match_details_clean
set city = case
when venue = 'Dubai International Cricket Stadium' then 'Dubai'
when venue= 'Sharjah Cricket Stadium'then 'Sharjah'
else city
end;

-- After making all the required changes in the table I inserted the data into the venue_dim table --
insert into ipl_database.consumption.venue_dim(venue_name,city)
select distinct venue,city
from ipl_database.clean.match_details_clean
group by
venue, city
order by venue;

select distinct venue, city from ipl_database.clean.match_details_clean
order by venue; 

--Updated the match_type_dim table with the distinct values--
insert into ipl_database.consumption.match_type_dim (match_type)
select 
match_type 
from ipl_database.clean.match_details_clean 
group by match_type;


--  Inserted the values into the play_off_dim with the distinct values from the match details table--
insert into ipl_database.consumption.play_off_dim(play_offs)
select 
play_offs
from ipl_database.clean.match_details_clean
group by play_offs
order by play_offs;


--Now as we need to insert the values into  the date_dim table we need to know the first match and most recent match dates so that we can create the date_dim;

select min (date), max(date) from ipl_database.clean.match_details_clean;
--min date = 2008-04-18
--max date = 2024-05-26;

-- first i created a temporary table to create the date_dim table ;

CREATE or replace transient TABLE ipl_database.consumption.date_rnage01 (date DATE) ;

--Inserted the values from the min date to max date --
INSERT INTO ipl_database.consumption.date_rnage01(date)
WITH RECURSIVE date_range AS (
    SELECT '2008-04-18'::DATE AS date_column
    UNION ALL
    SELECT date_column + INTERVAL '1 DAY'
    FROM date_range
    WHERE date_column < '2024-05-26'
)
SELECT date_column
FROM date_range;


--Finally I inserted into our main date_dim , what ever we needed from the above table--
INSERT INTO ipl_database.consumption.date_dim (Date_ID, Full_Dt, Day, Month, Year, Quarter, DayOfWeek, DayOfMonth, DayOfYear, DayOfWeekName, IsWeekend)
SELECT
ROW_NUMBER() OVER (ORDER BY Date) AS DateID, 
Date AS FullDate,
EXTRACT(DAY FROM Date) AS Day, 
EXTRACT (MONTH FROM Date) AS Month, 
EXTRACT (YEAR FROM Date) AS Year,
CASE WHEN EXTRACT (QUARTER FROM Date) IN (1, 2, 3, 4) THEN EXTRACT (QUARTER FROM Date) END AS Quarter,
DAYOFWEEKISO(Date) AS DayOfWeek,
EXTRACT(DAY FROM Date) AS DayOfMonth,
DAYOFYEAR (Date) AS DayOfYear, DAYNAME (Date) AS DayOfWeekName,
CASE When DAYNAME (Date) IN ('Sat', 'Sun') THEN 1 ELSE 0 END AS IsWeekend
FROM ipl_database.consumption.date_rnage01;

select * from ipl_database.clean.delivery_clean_tbl;

--Now I deleted the table which is no more required --
DROP TABLE ipl_database.consumption.date_rnage01;

select * from ipl_database.clean.match_details_clean;
select * from ipl_database.consumption.match_fact;

-- Finally I inserted all the numeric values into our main fact table match_fact table --
insert into ipl_database.consumption.match_fact
select
m.match_number as match_id,
sd.season_id,
m.season_match_number,
pd.play_off_id,
dd.date_id,
rd.referee_id,
tud.tv_umpire_id,
ud.umpire_id,
ud1.umpire_id,
ftd.team_id,
std.team_id,
vd.venue_id,
mt.match_type_id,
20 as total_overs,
6 as balls_per_overs,
max(case when d. team_name = m. first_team then d.over else 0 end) as OVERS_PLAYED_BY_TEAM_A,
sum(case when d. team_name = m. first_team then 1 else 0 end) as BALLS_PLAYED_BY_TEAM_A,
round(sum(case when d. team_name = m.first_team then d.extras else 0 end),0) as EXTRA_BOWLS_PLAYED_BY_TEAM_A,
round(sum(case when d. team_name = m. first_team then d.extra_runs else 0 end),0) as EXTRA_RUNS_SCORED_BY_TEAM_A,
sum(case when d. team_name = m.first_team and runs between 4 and 5 then 1 else 0 end) fours_by_team_a,
sum(case when d. team_name = m.first_team and runs >= 6 then 1 else 0 end) sixes_by_team_a,
round((sum(case when d. team_name = m. first_team then d.runs else 0 end) + sum(case when d. team_name = m.first_team then d. extra_runs else 0 end)),0) as total_runs_scored_BY_TEAM_A,
sum(case when d. team_name = m. first_team and d.player_out is not null then 1 else 0 end) as WICKET_LOST_BY_TEAM_A,

max(case when d. team_name = m. second_team then d.over else 0 end) as OVERS_PLAYED_BY_TEAM_B,
sum(case when d. team_name = m. second_team then 1 else 0 end) as BALLS_PLAYED_BY_TEAM_B,
sum(case when d. team_name = m.second_team then d.extras else 0 end) as EXTRA_BOWLS_PLAYED_BY_TEAM_B,
round(sum(case when d. team_name = m. second_team then d.extra_runs else 0 end),0) as EXTRA_RUNS_SCORED_BY_TEAM_B,
round(sum(case when d. team_name = m.second_team and runs between 4 and 5 then 1 else 0 end),0) fours_by_team_b,
sum(case when d. team_name = m.second_team and runs >= 6 then 1 else 0 end) sixes_by_team_b,
round((sum(case when d. team_name = m. second_team then d.runs else 0 end) + sum(case when d. team_name = m. second_team then d. extra_runs else 0 end)),0) as total_runs_scored_BY_TEAM_B,
sum(case when d. team_name = m. second_team and d.player_out is not null then 1 else 0 end) as WICKET_LOST_BY_TEAM_B,
tw. team_id as toss_winner_team_id,
toss_decision as toss_decision, 
matach_result as match_result, 
mw.team_id as winner_team_id,

from ipl_database.clean.match_details_clean m
join date_dim dd on m.date = dd.full_dt
join season_dim sd on m.season = sd.season
join referee_dim rd on m.match_refree = rd.refere
join match_type_dim mt on m.match_type = mt.match_type
join play_off_dim pd on m.play_offs = pd.play_offs
join tv_umpire_dim tud on m.tv_umpire = tud.tv_umpire
join umpires_dim ud on m.umpire1 = ud.umpire
join umpires_dim ud1 on m.umpire2 = ud1.umpire
join team_dim ftd on m.first_team = ftd.team_name
join team_dim std on m.second_team = std.team_name
join venue_dim vd on m.venue = vd.venue_name
join ipl_database.clean.delivery_clean_tbl d on m.match_number = d.match_id
join team_dim tw on m. toss_winner = tw. team_name
join team_dim mw on m.winner= mw. team_name
group by
match_number,
dd.date_id,
sd.season_id,
m.season_match_number,
mt.match_type_id,
rd.referee_id,
pd.play_off_id,
tud.tv_umpire_id,
ud.umpire_id,
ud1.umpire_id,
ftd.team_id,
std.team_id,
vd.venue_id,
tw. team_id,
toss_decision,
matach_result,
mw.team_id;

-- I found that each match match has only one record and all are numeric and I found the row count as 1095 --
select count(*) from match_fact; --1095

--Now we need to create one final fact table which stores the details about all delivery details --

-- To do that firat I created table manually --
CREATE or replace TABLE delivery_fact (
deleviry_id int,
match_id INT , 
team_id INT, 
bowler_id INT, 
batter_id INT, 
non_striker_id INT, 
over INT, 
runs INT,
extra_runs INT,
extya_type VARCHAR (255) , 
player_out VARCHAR (255) , 
player_out_kind VARCHAR (255) ,
CONSTRAINT fk_del_match_id FOREIGN KEY (match_id) REFERENCES match_fact(match_id),
CONSTRAINT fk_del_team FOREIGN KEY (team_id) REFERENCES team_dim(team_id),
CONSTRAINT fk_bowler FOREIGN KEY (bowler_id) REFERENCES player_dim(player_id),
CONSTRAINT fk_batter FOREIGN KEY (batter_id) REFERENCES player_dim (player_id),
CONSTRAINT fk_stricker FOREIGN KEY (non_striker_id) REFERENCES player_dim (player_id)
);

-- And finally I inserted the details into the fact table --

insert into delivery_fact 
select
delivery_id,
match_id, 
td. team_id, 
bpd.player_id as bower_id, 
spd.player_id as batter_id, 
nspd.player_id as non_stricker_id,
d. over,
d.runs,
case when d. extra_runs is null then 0 else d. extra_runs end as extra_runs, 
case when d. extra_type is null then 'None' else d.extra_type end as extra_type, 
case when d. player_out is null then 'None' else d. player_out end as player_out, 
case when d. player_out_kind is null then 'None' else d. player_out_kind end as player_out_kind
from
ipl_database.clean.delivery_clean_tbl d
join team_dim td on d. team_name = td. team_name
join player_dim bpd on d. bowler = bpd. player_name and d.season = bpd.season_name
join player_dim spd on d. batter = spd. player_name and d.season = spd.season_name
join player_dim nspd on d. non_striker = nspd. player_name and d.season = nspd.season_name
order by delivery_id;

select count(*) from delivery_fact;--261701

--261701 -->(1095*40*6=262800) nearly equal




