-- set the context

use rorle sysadmin;
use database tisdb;

use schema ch_10;
use warehouse ch10_demo_wh;
alter session set quer_tag = 'chapter-10';  --search about it


// ================================


     -- step 1
    --  lets create our first table called myc_custome
    -- first before we apply auto ingest (delta file )
    create or replace table my_customer(
        cust_key number(38,0),
        name varchar(25),
        address varchar(40),
        natin_key naumber(38,0),
        phone varchar(15),
        account_balance number (12,2),
        market_segment varchar(10),
        comment varchar(117)
    );

    -- check if it has any record
    select * from my_customer; -- no data


    -- **********************
    -- step - 02
    -- lets create a simple csv file format as we  are using csv as delta file
    -- copy command also seen that copy command needs a file format.

    create or replace file_format cav_ff type='csv'
    fields_optionally_anclosed_by = '\042';

    -- before check if stg exist and has data
    show stages like '%_010%';

    







// #######################################################################
-- copied  from the mediun blog of DE simplifies
use role sysadmin;
use database tipsdb;
create schema ch10;

CREATE WAREHOUSE ch10_demo_wh WITH WAREHOUSE_SIZE = 'MEDIUM' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 1 SCALING_POLICY = 'STANDARD' COMMENT = 'this is demo warehouses';
use warehouse ch10_demo_wh;

alter session set query_tag ='chapter-10';

-- you can craete named stages and then list them using list command
    show stages;
    list @STG01;
    
-- very simple contruct for internal stage
    CREATE STAGE "TIPSDB"."CH09".stg03 COMMENT = 'This is my demo internal stage';
    
-- if you have lot of stages, then you can use like 
    show stages like '%03%';
    
    show stages like '%s3%';
    -- if it has credential, it will show

-- very simple contruct for internal stage
    CREATE STAGE "TIPSDB"."CH09".stg03 COMMENT = 'This is my demo internal stage';
    
-- if you have lot of stages, then you can use like 
    show stages like '%03%';
    
    show stages like '%s3%';

    list @~ pattern='.*test.*';
    list @~ pattern='.*.gz';
    list @~ pattern='.*.html';

    show stages like 'TIPS_S3_EXTERNAL_STAGE';
    list @TIPS_S3_EXTERNAL_STAGE;

drop table customer_parquet_ff;
    create or replace table customer_parquet_ff(
        my_data variant
    ) 
    STAGE_FILE_FORMAT = (TYPE = PARQUET);

list @%customer_parquet/;
    -- now lets query the data using $ notation
    select 
        metadata$filename, 
        metadata$file_row_number,
        $1:CUSTOMER_KEY::varchar,
        $1:NAME::varchar,
        $1:ADDRESS::varchar,
        $1:COUNTRY_KEY::varchar,
        $1:PHONE::varchar,
        $1:ACCT_BAL::decimal(10,2),
        $1:MKT_SEGMENT::varchar,
        $1:COMMENT::varchar
        from @%customer_parquet_ff ;

copy into customer_parquet_ff from @%customer_parquet_ff/customer.snappy.parquet;
      
      select * from  customer_parquet_ff;

copy into customer_parquet_ff 
        from @%customer_parquet_ff/customer.snappy.parquet
        force=true;

drop table my_customer;
    create or replace table my_customer (
        CUST_KEY NUMBER(38,0),
        NAME VARCHAR(25),
        ADDRESS VARCHAR(40),
        NATION_KEY NUMBER(38,0),
        PHONE VARCHAR(15),
        ACCOUNT_BALANCE NUMBER(12,2),
        MARKET_SEGMENT VARCHAR(10),
        COMMENT VARCHAR(117)
    );
    
    --lets see if it has any record
    select * from my_customer; -

   create or replace file format csv_ff type = 'csv'
    field_optionally_enclosed_by = '\042';

-- before check if stg exist and has data
    show stages like '%_010%';
    
    -- let's create it.
    create or replace stage stg_010 
        file_format = csv_ff
        comment = 'This is my internal stage for ch-10';
    
    -- let's describe the stage using desc sql command
    desc stage stg_010;

list @stg_010/ pattern='.*.csv';
    
    -- ***********************************
    -- Step-05
    -- load into table
    copy into my_customer from @stg_010/history;

-- create a pipe object & understand its construct. 
    drop pipe my_pipe_10;
    create or replace pipe my_pipe_10 
    as 
    copy into my_customer from @stg_010/delta;
    
    -- describe the pipe object 
    desc pipe my_pipe_10;

select * from table(validate_pipe_load(
      pipe_name=>'my_pipe_10',
      start_time=>dateadd(hour, -1, current_timestamp())));

   alter pipe my_pipe_10 set pipe_execution_paused = false;

alter pipe my_pipe_10 refresh prefix='/customer_10*' modified_after='2021-11-01T13:56:46-07:00';



