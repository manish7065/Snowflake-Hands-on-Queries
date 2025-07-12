-- connectin to the snowflake from terminal

snowsql -a wxrtvol-et53032  -u maiage96 -d our_first_db -s manage_db.internal_schema.local_emp_stg -q \
"PUT employee_data_1.csv @local_emp_stg  AUTO_COMPRESS=TRUE;"


-- connct to the snowsql first (it will ask for pwd to connect)
snowsql -a wxrtvol-et53032  -u maiage96 -d our_first_db -s manage_db.internal_schema.local_emp_stg 

-- file path 
nicolai/Section 11 - Snowpipe/75 Creating stage/employee_data_1.csv
/Users/manu/Desktop/Snowflake_study/Hands-on Queries/nicolai/Section 11 - Snowpipe/75 Creating stage/employee_data_1.csv

snowsql -a wxrtvol-et53032  -u maiage96 -d our_first_db -s manage_db.internal_schema.local_emp_stg -q \
"PUT nicolai/Section\ 11 - Snowpipe/75 Creating stage/employee_data_1.csv @local_emp_stg  AUTO_COMPRESS=TRUE;"

nicolai/Section\ 11\ -\ Snowpipe/75\ Creating\ stage/employee_data_1.csv 

snowsql -a wxrtvol-et53032  -u maiage96 -d our_first_db -s manage_db.internal_schema.local_emp_stg -q \
"PUT nicolai/Section\ 11\ -\ Snowpipe/75\ Creating\ stage/employee_data_1.csv @local_emp_stg  AUTO_COMPRESS=TRUE;"

snowsql -a wxrtvol-et53032 -u maiage96 -d OUR_FIRST_DB -s MANAGE_DB.INTERNAL_SCHEMA -q \
"PUT file://nicolai/Section\ 11\ -\ Snowpipe/75\ Creating\ stage/employee_data_1.csv @local_emp_stg AUTO_COMPRESS=TRUE;"

snowsql -a wxrtvol-et53032 -u maiage96 -d OUR_FIRST_DB -s MANAGE_DB.INTERNAL_SCHEMA -q \
"PUT 'file://nicolai/Section 11 - Snowpipe/75 Creating stage/employee_data_1.csv' @local_emp_stg AUTO_COMPRESS=TRUE;"

-- All the above commands having some issues
-- Working command
snowsql -a wxrtvol-et53032 -u maiage96 -d OUR_FIRST_DB -s MANAGE_DB.INTERNAL_SCHEMA -q \
"USE SCHEMA MANAGE_DB.INTERNAL_SCHEMA; PUT 'file://nicolai/Section 11 - Snowpipe/75 Creating stage/employee_data_1.csv' @local_emp_stg AUTO_COMPRESS=TRUE;"

