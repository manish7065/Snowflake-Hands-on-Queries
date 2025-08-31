create or replace stage manage_db.external_schm.json_stage
 url='s3://bucketsnowflake-jsondemo';


CREATE OR REPLACE FILE FORMAT MANAGE_DB.external_schm.json_format
TYPE = JSON;

create or replace table our_first_db.public.json_raw
(
raw_file variant
);  

show stages;

copy into our_first_db.public.json_raw
from @manage_db.external_schm.json_stage
file_format = MANAGE_DB.external_schm.json_format
FILES=('HR_data.json');

SELECT * FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

---------------------   PARSING THE DATA
SELECT * FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT RAW_FILE:city FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;
SELECT RAW_FILE:first_name FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT RAW_FILE:spoken_languages::string as languages FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;
SELECT RAW_FILE:spoken_languages:language FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

-- fetching multiple parameters
select 
    RAW_FILE:id::int ID,
    RAW_FILE:first_name::string FirstName,
    RAW_FILE:last_name::string LastName,
    RAW_FILE:gender::string Gender,
    
    FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

select raw_file:job from our_first_db.public.json_raw;

-- Handling nested data
select 
raw_file:first_name::string as first_name,
raw_file:job.title::string as role,
raw_file:job.salary::int as salary,

from our_first_db.public.json_raw;


select * from our_first_db.public.json_raw;

select ARRAY_SIZE(RAW_FILE:prev_company) AS LEN,
RAW_FILE:prev_company 
from our_first_db.public.json_raw;


select raw_file:spoken_languages from our_first_db.public.json_raw;

KOl09ol


select raw_file:spoken_languages[0].language from our_first_db.public.json_raw;

select f.value:language as language from our_first_db.public.json_raw, table(flatten(raw_file:spoken_languages)) f;

select f.value:level as level from our_first_db.public.json_raw, table(flatten(raw_file:spoken_languages)) f; 


-- creating new table language for storing languages vlaues after oparsing
create or replace table language as
select raw_file:first_name as first_name,
f.value:language as first_language,
f.value:level as level_spoken
from our_first_db.public.json_raw, table(flatten(raw_file:spoken_languages)) f;


select * from language;

truncate table language;


insert into language
select raw_file:first_name as first_name,
f.value:language as first_language,
f.value:level as language_level
from json_raw, table(flatten(raw_file:spoken_languages)) f;

select * from language;
