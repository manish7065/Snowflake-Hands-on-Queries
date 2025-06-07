//Creating the table / Meta data


CERATE DATABASE OUR_FIRST_DB;
USE DATABASE OUR_FIRST_DB;

CREATE TABLE "OUR_FIRST_DB"."PUBLIC"."LOAN_PAYMENT" (
  "Loan_ID" STRING,
  "loan_status" STRING,
  "Principal" STRING,
  "terms" STRING,
  "effective_date" STRING,
  "due_date" STRING,
  "paid_off_time" STRING,
  "past_due_days" STRING,
  "age" STRING,
  "education" STRING,
  "Gender" STRING);
  
  
 //Check that table is empy
 USE DATABASE OUR_FIRST_DB;

 SELECT * FROM LOAN_PAYMENT;

 
 //Loading the data from S3 bucket
  
 COPY INTO LOAN_PAYMENT
    FROM s3://bucketsnowflakes3/Loan_payments_data.csv
    file_format = (type = csv 
                   field_delimiter = ',' 
                   skip_header=1);
    

//Validate
 SELECT * FROM LOAN_PAYMENT;

//Check the number of records
SELECT COUNT(*) FROM LOAN_PAYMENT;

-- trying to load data from "LOAN_PAYMENT" to GCS storage
-- successfully loaded data into gcs bucket (pubic bucket)


CREATE STORAGE INTEGRATION gcs_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://96_p_bucket/general_dir/');


CREATE OR REPLACE STAGE my_gcs_stage
  URL = 'gcs://96_p_bucket/general_dir/'
  STORAGE_INTEGRATION = gcs_int;


COPY INTO @my_gcs_stage/data.csv
FROM LOAN_PAYMENT
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"')
HEADER = TRUE;


SELECT * FROM LOAN_PAYMENT;