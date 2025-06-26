-- in this page i have fetches data from s3 and queried it vefore loading and loaded it to a table in our_first_db
-- main challenges was the file formats
-- fileformat along with the query to query the external table is not possible 
-- for custom type of file format namd file format shold have to be used



create database Manage_DB;
use Manage_db; --in web ui interface queries are not case sensetive

create stage external_stg;

create schema external_schm;

create or replace STAGE manage_db.external_schm.aws_stg
url='s3://bucketsnowflakes3'
credentials = (aws_key_id='ABCD_DUMMY_ID',aws_secret_key='1234abcd_key');


desc stage manage_db.external_schm.aws_stg;

-- alter the stage
alter stage manage_db.external_schm.aws_stg
set credentials=(aws_key_id='xyz_123',aws_secret_key='abc123');


-- no credentials required for the publically available bucket
create or replace stage manage_db.external_schm.aws_stg
url='s3://bucketsnowflakes3';

-- url of the bucket
-- https://bucketsnowflakes3.s3.us-east-1.amazonaws.com/

desc stage aws_stg;

LIST @aws_stg;
-- nofile format allowed for the external stage data querries
-- SELECT *
-- FROM @aws_stg/sampledata.csv
-- (FILE_FORMAT => (TYPE => 'CSV' SKIP_HEADER = 1));


-- SELECT $1, 
-- FROM @aws_stg/sampledata.csv
-- (FILE_FORMAT => (TYPE => 'CSV', SKIP_HEADER => 1));

-- quering the external stage data with specific file format is only possible with named file format
CREATE OR REPLACE FILE FORMAT tab_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  ;

SELECT $1, $2
FROM @aws_stg/sampledata.csv
(FILE_FORMAT => 'tab_format');

SELECT $1, $2, $3
FROM @aws_stg/OrderDetails.csv
(FILE_FORMAT => 'tab_format');


SELECT $1, $2,
FROM @aws_stg/sampledata.csv;

create database our_first_db;

create or replace table our_first_db.public.orders
(
order_id char(20),
amount int,
profit int,
quantity int,
category char(50),
sub_category char(50)
);

COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS
    FROM @aws_stg
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern ='.*Order.*'
    ON_ERROR = 'CONTINUE';


COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS
FROM @aws_stg
FILE_FORMAT = (
  TYPE = 'CSV',
  FIELD_DELIMITER = ',',
  SKIP_HEADER = 1,
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
)
PATTERN = '.*Order.*'
ON_ERROR = 'CONTINUE';


select * from our_first_db.public.orders;

-- loading the loan mayment table data
-- columns: Loan_ID,loan_status,Principal,terms,effective_date,due_date,paid_off_time,past_due_days,age,education,Gender

list @aws_stg;

-- first query the external table
select $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12 from @aws_stg/Loan_payments_data.csv;

-- - quering the external data using the custom named file format

create file format ext_loan_file_format
type='csv',
field_delimiter = ',';

select  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12 from @aws_stg/Loan_payments_data.csv
(file_format => 'ext_loan_file_format');


create or replace table our_first_db.public.loan_payment
(
loan_id char(20),
loan_status char(30),
principal int,
terms int,
effective_date date,
due_date date,
paid_off_time char(20),
past_due_date char(50),
age int,
educatin char(50),
gender char(10)
);

copy into our_first_db.public.loan_payment
from @aws_stg
file_format = (type='csv',skip_header=1, field_delimiter =',')
pattern = '.*Loan.*'
on_error = 'continue'
;

select * from our_first_db.public.loan_payment;