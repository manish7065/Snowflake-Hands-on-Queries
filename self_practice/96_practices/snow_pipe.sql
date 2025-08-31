CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.employees (
  id INT,
  first_name STRING,
  last_name STRING,
  email STRING,
  location STRING,
  department STRING
  );

-- create a file format 
CREATE OR REPLACE FILE FORMAT MANAGE_DB.EXTERNAL_SCHM.CSV_FILE_FORMAT
TYPE = CSV
FIELD_DELIMITER =","
skip_header = 1
null_if = ('null','Null','NULL')
empty_field_as_null = True;

-- # to learn handeling null values 
SELECT PARSE_JSON(NULL) AS "SQL NULL",
       PARSE_JSON('null') AS "JSON NULL",
       PARSE_JSON('[ null ]') AS "JSON NULL",
       PARSE_JSON('{ "a": null }'):a AS "JSON NULL",
       PARSE_JSON('{ "a": null }'):b AS "ABSENT VALUE";

SELECT PARSE_JSON('{ "a": null }'):a,
       TO_CHAR(PARSE_JSON('{ "a": null }'):a);

show file formats;

create stage manage_db.external_schm.csv_aws_stg
url= 's3://snowflakes3bucket123/csv/snowpipe'
-- storage_integration = s3_int
file_format = manage_db.external_schm.csv_file_format;


show storage_integration;

LIST @manage_db.external_schm.csv_aws_stg;

-- first loading data from local to stage local_emp_stg
create schema manage_db.internal_schema;
create stage manage_db.internal_schema.local_emp_stg;


list @manage_db.internal_schema.local_emp_stg;

-- wrong file path syntex
select * from @manage_db.internal_schema.local_emp_stg.employee_data_1.csv.gz;

-- working syntex
SELECT  $1, $2, $3, $4  FROM @manage_db.internal_schema.local_emp_stg/employee_data_1.csv.gz;


-- creating schema for pipe
create or replace schema manage_db.pipe_schema;

-- create pipe
create or  replace pipe manage_db.pipe_schema.employee_pipe
auto_ingest = TRUE
as
copy into our_first_db.public.employees
from @manage_db.internal_schema.local_emp_stg;



