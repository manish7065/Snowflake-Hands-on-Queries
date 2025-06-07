-- set context
use role sysadmin;
use database FILMS_DB;
USE SCHEMA films_schema;

-- create json file format object with default options
CREATE OR REPLACE FILE FORMAT JSON_FILE_FORMAT
TYPE='JSON',
FILE_EXTENSION=NULL,
DATE_FORMAT='AUTO',
TIME_FORMAT='AUTO',
TIMESTAMP_FORMAT='AUTO',
BINARY_FORMAT='HEX',
TRIM_SPACE=FALSE,
NULL_IF='',
COMPRESSION='AUTO',
ENABLE_OCTAL=FALSE,
ALLOW_DUPLICATE=FALSE,
STRIP_OUTER_ARRAY=FALSE,
STRIP_NULL_VALUES=FALSE,
IGNORE_UTF8_ERRORS=FALSE,
REPLACE_INVALID_CHARACTERS=FALSE,
SKIP_BITE_ORDER_MARK=TRUE;

CREATE OR REPLACE FILE FORMAT JSON_FILE_FORMAT
TYPE='JSON',
FILE_EXTENSION=NULL,
DATE_FORMAT='AUTO',
TIME_FORMAT='AUTO',
TIMESTAMP_FORMAT='AUTO',
BINARY_FORMAT='HEX',
TRIM_SPACE=FALSE,
NULL_IF='',
COMPRESSION='AUTO',
ENABLE_OCTAL=FALSE,
ALLOW_DUPLICATE=FALSE,
STRIP_OUTER_ARRAY=FALSE,
STRIP_NULL_VALUES=FALSE,
IGNORE_UTF8_ERRORS=FALSE,
REPLACE_INVALID_CHARACTERS=FALSE,
SKIP_BYTE_ORDER_MARK=TRUE;


show tables;
select * from films_elt;

select json_variant from films_elt;

truncate table films_elt;

COPY INTO FILMS_ELT
FROM @FILMS_STAGE/films.json
FILE_FORMAT = JSON_FILE_FORMAT;


SELECT 
json_variant:id as id,
json_variant:title as title,
json_variant:release_date as release_date,
json_variant:release_date::date as release_date_dd_cast,
to_date(json_variant:release_date) as release_date_func_cast,
json_variant:actors as actors,
json_variant:actors[0] as first_actor,
json_variant:ratings as ratings,
json_variant:ratings.imdb_ratings as IMDB_ratings,
from films_elt
where release_date>=date('2000-01-01');



WHERE release_date >= date('2000-01-01');

select json_variant:id as id from films_elt; --returning null
select
json_variant[0]:actors[0] as first_actor from films_elt; -- returning "Ellija Wood"
json_variant[]:actors[0] as first_actor from films_elt; -- returning "Ellija Wood"

select json_variant[0]['id'] as id from films_elt; -- working

SELECT json_variant:id::STRING AS id
FROM films_elt;

SELECT value:id::STRING AS id
FROM films_elt,
     LATERAL FLATTEN(input => json_variant);

-- all working ways to explore the json
SELECT 
value:id as id, 
value:title as title, 
value:release_date::date AS release_date, 
value:actors[0] as first_actor,
value:ratings.imdb_rating AS IMDB_rating
FROM FILMS_ELT,
lateral flatten(input => json_variant);

SELECT 
value:id as id, 
value:title as title, 
value:release_date AS release_date, 
value:release_date::date AS release_date_dd_cast, 
to_date(value:release_date) AS release_date_func_cast,
value:actors AS actors,
value:actors[0] as first_actor,
value:ratings AS ratings,
value:ratings.imdb_rating AS IMDB_rating
FROM FILMS_ELT,
lateral flatten(input => json_variant)
WHERE release_date >= date('2000-01-01');


SELECT 
json_variant[0]['id'] as id, 
json_variant['title'] as title, 
json_variant['release_date']::date AS release_date, 
json_variant['actors'][0] as first_actor,
json_variant['ratings']['imdb_rating'] AS IMDB_rating
FROM FILMS_ELT
WHERE release_date >= date('2000-01-01');


-- Due to [] with films.json now trying without [] on films2.json


CREATE OR REPLACE TABLE FILMS_ELT2 (
JSON_VARIANT VARIANT
);

COPY INTO FILMS_ELT2
FROM @FILMS_STAGE/films2.json
FILE_FORMAT = JSON_FILE_FORMAT;
select * from films_elt2;



SELECT 
value:id as id, 
value:title as title, 
value:release_date AS release_date, 
value:release_date::date AS release_date_dd_cast, 
to_date(value:release_date) AS release_date_func_cast,
value:actors AS actors,
value:actors[0] as first_actor,
value:ratings AS ratings,
value:ratings.imdb_rating AS IMDB_rating
FROM FILMS_ELT,
lateral flatten(input => json_variant)
WHERE release_date >= date('2000-01-01');

-- flatten table function

select json_variant:title, json_variant:ratings from films_ELT2
limit 1; -- returning null due to  [] 
;
select * from table(flatten(input => json_variant:ratings from films_elt limit 1 ));

SELECT * FROM TABLE(FLATTEN(INPUT => select json_variant:ratings from films_ELT limit 1)) ;

SELECT VALUE FROM TABLE(FLATTEN(INPUT => select json_variant:ratings from films_ELT limit 1));



-- Lateral Flatten
SELECT 
json_variant:title,
json_variant:release_date::date,
L.value 
FROM FILMS_ELT F,
LATERAL FLATTEN(INPUT => F.json_variant:ratings) L
LIMIT 2;
 

desc table films_elt2;


select $1 from @films_stage/films.json; -- its working
select $1:id from @films_stage/films.json; -- its not working

select $2 from @films_stage/films.json ;
select $2 from @films_stage/films.json  FILE_FORMAT=json_file_format;

SELECT * FROM FILMS_DB.FILMS_SCHEMA.FILE_FORMATS;
SELECT * 
FROM FILMS_DB.INFORMATION_SCHEMA.FILE_FORMATS;
DESC FILE_FORMAT JSON_FILE_FORMAT;

DESC FILE FORMAT FILMS_DB.FILMS_SCHEMA.JSON_FILE_FORMAT;

select $1, $2 from @films_stage/films.json;


-- Creating new table with distinguish columns instead of collective column as json_arient
create or replace table file_etl_dst(
ID STRING,
TITLE STRING,
RELEASE_DATE DATE,
STARS ARRAY,
RATINGS OBJECT
);

COPY INTO file_etl_dst FROM
(SELECT
$1:id,
$1:title,
$1:release_date::date,
$1:actors,
$1:ratings
from @films_stage/films2.json)
file_format = json_file_format
force=true;

select * from file_etl_dst;

-- Displaying first_name and  imdb ratings from the respective array column
select id, title, release_date,
stars[0]::string as first_star,
¯`ratings['imdb_rating'] as rating
from file_etl_dst;

-- trying to load the data by matching the column names
truncate table file_etl_dst;

copy into file_etl_dst
from  @films_stage/film2.json
file_format = json_file_format
MATCH_BY_COLUMN_NAME = CASE_INSENSETIVE
force=true;


