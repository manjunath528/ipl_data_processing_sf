--Now we are transforming the data into the new stage --

-- First we need to give the file format with which it has to be stored --

create or replace file format unload_csv_format
type = csv
compression = 'NONE'
field_delimiter = ','
record_delimiter = '\n'
file_extension = 'csv'
field_optionally_enclosed_by = '\042';

-- Now I have created the stage to store the files--
create or replace stage consumption.my_stg;

list @my_stg;

-- Finally I transfered all the files into the stage --

COPY INTO @my_stg/ipl_data/date_dim
FROM ipl_database.consumption.date_dim
FILE_FORMAT = (format_name='unload_csv_format')
HEADER = true;

COPY INTO @my_stg/ipl_data/delivery_fact
FROM ipl_database.consumption.delivery_fact
FILE_FORMAT = (format_name='unload_csv_format')
HEADER = true;

COPY INTO @my_stg/ipl_data/match_fact
FROM ipl_database.consumption.match_fact
FILE_FORMAT = (format_name='unload_csv_format')
HEADER = true;

COPY INTO @my_stg/ipl_data/match_type_dim
FROM ipl_database.consumption.match_type_dim
FILE_FORMAT = (format_name='unload_csv_format')
HEADER = true;

COPY INTO @my_stg/ipl_data/player_dim
FROM ipl_database.consumption.player_dim
FILE_FORMAT = (format_name='unload_csv_format')
HEADER = true;

COPY INTO @my_stg/ipl_data/play_off_dim
FROM ipl_database.consumption.play_off_dim
FILE_FORMAT = (format_name='unload_csv_format')
HEADER = true;

COPY INTO @my_stg/ipl_data/referee_dim
FROM ipl_database.consumption.referee_dim
FILE_FORMAT = (format_name='unload_csv_format')
HEADER = true;

COPY INTO @my_stg/ipl_data/season_di
FROM ipl_database.consumption.season_dim
FILE_FORMAT = (format_name='unload_csv_format')
HEADER = true;


COPY INTO @my_stg/ipl_data/team_dim
FROM ipl_database.consumption.team_dim
FILE_FORMAT = (format_name='unload_csv_format')
HEADER = true;

COPY INTO @my_stg/ipl_data/tv_umpire_dim
FROM ipl_database.consumption.tv_umpire_dim
FILE_FORMAT = (format_name='unload_csv_format')
HEADER = true;

COPY INTO @my_stg/ipl_data/umpires_dim
FROM ipl_database.consumption.umpires_dim
FILE_FORMAT = (format_name='unload_csv_format')
HEADER = true;

COPY INTO @my_stg/ipl_data/venue_dim
FROM ipl_database.consumption.venue_dim
FILE_FORMAT = (format_name='unload_csv_format')
HEADER = true;

--Now to check weather all files have moved to stage or not --
list @my_stg/ipl_data/;
