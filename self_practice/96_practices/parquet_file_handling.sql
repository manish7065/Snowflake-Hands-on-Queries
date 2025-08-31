-- creating anew file format to load the parquet data 
 create file format manage_db.external_schm.parquet_file_format
 type = 'parquet';

 show file formats;

 -- creating external stages

 create or replace stage manage_db.external_schm.parquet_stg
 url = 's3://snowflakeparquetdemo'
 file_format = manage_db.external_schm.parquet_file_format;

list @manage_db.external_schm.parquet_stg;
 
select $1 from @manage_db.external_schm.parquet_stg;
select * from @manage_db.external_schm.parquet_stg;

-- file fomat in queries 
select * from @manage_db.external_schm.parquet_stg
(file_format => manage_db.external_schm.parquet_file_format);


-- synatax for quering semi structured data
select $1:__index_level_0__  from @manage_db.external_schm.parquet_stg;

select DATE(365*60*60*24);


SELECT 
$1:__index_level_0__,
$1:cat_id,
$1:date,
$1:"__index_level_0__",
$1:"cat_id",
$1:"d",
$1:"date",
$1:"dept_id",
$1:"id",
$1:"item_id",
$1:"state_id",
$1:"store_id",
$1:"value"
FROM @MANAGE_DB.external_schm.parquet_stg;  

SELECT *
FROM @manage_db.external_schm.parquet_stg
LIMIT 10;



--queries with conversions and aliases

select 
$1:__index_level_0__::int as index_level,
$1:cat_id::varchar(50) as category,
DATE($1:date::int) as date,
$1:"dept_id"::VARCHAR(50) as Dept_ID,
$1:"id"::VARCHAR(50) as ID,
$1:"item_id"::VARCHAR(50) as Item_ID,
$1:"state_id"::VARCHAR(50) as State_ID,
$1:"store_id"::VARCHAR(50) as Store_ID,
$1:"value"::int as value
FROM @MANAGE_DB.EXTERNAL_schm.PARQUET_STg;


-- adding metadata 
SELECT $1:__index_level_0__::INT AS index_level,
$1:cat_id::varchar(50) as category,
DATE($1:date::INT) AS date,
$1:dept_id::varchar(50) as dept_id,
$1:id::varchar(50) as id,
$1:item_id::varchar(50) as item_id,
$1:state_id::varchar(50) as state_id,
$1:store_id::varchar(50) as store_id,
$1:value::int as value,
METADATA$fiLename as filename,
metadata$file_row_number as row_number,
to_timestamp_ntz(current_timestamp) as load_date
from @manage_db.external_schm.parquet_stg;

SELECT CURRENT_TIMESTAMP;



-- CREATE DESTINATION TABLE
CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.PARQUET_DATA(
ROW_NUMBER INT,
INDEX_LEVEL INT,
CAT_ID VARCHAR(50),
DATE DATE,
DEPT_ID VARCHAR(50),
ID VARCHAR(50),
ITEM_ID VARCHAR(50),
STATE_ID VARCHAR(50),
STORE_ID VARCHAR(50),
VALUE INT,
LOAD_DATE TIMESTAMP DEFAULT TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
);

COPY INTO OUR_FIRST_DB.PUBLIC.PARQUET_DATA
FROM (
SELECT 
METADATA$FILE_ROW_NUMBER,
$1:__index_level_0__::int,
$1:cat_id::VARCHAR(50),
DATE($1:date::int ),
$1:"dept_id"::VARCHAR(50),
$1:"id"::VARCHAR(50),
$1:"item_id"::VARCHAR(50),
$1:"state_id"::VARCHAR(50),
$1:"store_id"::VARCHAR(50),
$1:"value"::int,
TO_TIMESTAMP_NTZ(current_timestamp)
FROM @MANAGE_DB.EXTERNAL_SCHM.PARQUET_STG);

SELECT * FROM OUR_FIRST_DB.PUBLIC.PARQUET_DATA;

SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.WEB_SITE T1;

select count(*) from snowflake_sample_data.tpcds_sf100tcl.catalog_page;

SELECT COUNT(*)
FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CALL_CENTER;

-- Large sample data
select * from snowflake_sample_data.tpcds_sf100tcl.catalog_page
limit 100;

SELECT * FROM OUR_FIRST_DB.PUBLIC.PARQUET_DATA limit 10;


