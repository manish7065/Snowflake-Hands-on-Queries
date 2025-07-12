



-- =============================================================================
-- Snowflake Tutorial: Chapter 7 - Tables, Schemas, and Databases
-- Source: Data Engineering Simplified YouTube Channel
--
-- This script contains all queries demonstrated in the lecture.
-- Comments are added to explain the purpose and context of each command.
-- =============================================================================


-- /////////////////////////////////////////////////////////////////////////////
-- Section 1: Setting Context and Container Hierarchy
-- /////////////////////////////////////////////////////////////////////////////

-- Set the current role for the session. The SYSADMIN role has the necessary
-- permissions to create databases and other objects.
USE ROLE SYSADMIN;


-- Create a new database. When a new database is created, Snowflake automatically
-- sets it as the active database for the current session.
CREATE DATABASE my_db
COMMENT = 'This is my demo db';

-- The context automatically changes. We can verify this using built-in functions.
SELECT CURRENT_ROLE(), CURRENT_DATABASE();

-- Create a new schema within the currently active database (MY_DB).
-- Similar to databases, creating a schema makes it the active one for the session.
CREATE SCHEMA my_schema
COMMENT = 'This is my demo schema under my_db';

-- We can verify the full context (role, database, and schema) has been updated.
SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA();

-- You can also explicitly set the context at any time.
USE DATABASE my_db;
USE SCHEMA my_schema;

-- The SHOW command can be used to list objects.
SHOW DATABASES;
SHOW SCHEMAS;

-- We can also use a LIKE clause to filter the results.
-- NOTE: Unquoted identifiers in Snowflake are case-insensitive and stored as uppercase.
SHOW DATABASES LIKE 'MY%';


-- /////////////////////////////////////////////////////////////////////////////
-- Section 2: Table Creation and Data Types
-- /////////////////////////////////////////////////////////////////////////////

-- == Subsection 2.1: Numeric Data Types ==
-- This section demonstrates creating a table with various numeric data types.

-- Drop the table first if it exists to ensure a clean run.
DROP TABLE IF EXISTS my_table;

-- In Snowflake, NUMBER, DECIMAL, NUMERIC, INT, and INTEGER are all aliases for
-- the base NUMBER type. If no precision/scale is given, it defaults to NUMBER(38, 0).
-- AUTOINCREMENT will automatically generate a sequence for the 'id' column.
CREATE TABLE my_table (
    id INT AUTOINCREMENT,
    num NUMBER,
    num10_1 NUMBER(10,1),
    decimal_20_2 DECIMAL(20,2),
    numeric_field NUMERIC(30,3), -- Renamed from 'numeric' to avoid keyword conflict
    int_field INT, -- Renamed from 'int' to avoid keyword conflict
    integer_field INTEGER -- Renamed from 'integer' to avoid keyword conflict
);

-- Use the DESCRIBE command to see the table's structure.
-- Notice how all numeric types are resolved to NUMBER with specific precision/scale.
DESC TABLE my_table;

-- The GET_DDL function is a powerful utility to see the exact DDL statement
-- that was used to create any object.
SELECT GET_DDL('table', 'my_table');


-- == Subsection 2.2: String/Text Data Types ==
-- In Snowflake, VARCHAR, CHAR, STRING, and TEXT are all functionally identical.
-- Specifying a length (e.g., VARCHAR(50)) is for ANSI compatibility and does
-- not affect performance or storage. All can store up to 16MB.
CREATE TABLE my_text_table (
    id INT AUTOINCREMENT,
    v VARCHAR,
    v50 VARCHAR(50),
    c CHAR,
    c10 CHAR(10),
    s STRING,
    s20 STRING(20),
    t TEXT,
    t30 TEXT(30)
);

DESC TABLE my_text_table;


-- == Subsection 2.3: Boolean Data Type ==
CREATE OR REPLACE TABLE my_boolean_table(
    b BOOLEAN,
    n NUMBER,
    s STRING
);

-- Snowflake correctly interprets various literal values for booleans.
-- Importantly, NULL is treated as NULL (unknown), not as FALSE.
INSERT INTO my_boolean_table VALUES
    (true, 1, 'yes'),
    (false, 0, 'no'),
    (null, null, null);

SELECT * FROM my_boolean_table;


-- == Subsection 2.4: Date and Timestamp Data Types ==
CREATE OR REPLACE TABLE my_ts_table(
    today_date DATE DEFAULT CURRENT_DATE(),
    now_time TIME DEFAULT CURRENT_TIME(),
    now_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Insert a record using functions.
INSERT INTO my_ts_table (today_date, now_time, now_ts) VALUES (CURRENT_DATE(), CURRENT_TIME(), CURRENT_TIMESTAMP());

-- Insert another record, this time letting the DEFAULT values apply automatically.
-- We do not provide a value for 'today_date'.
INSERT INTO my_ts_table (now_time, now_ts) VALUES (CURRENT_TIME(), CURRENT_TIMESTAMP());

SELECT * FROM my_ts_table;

-- You can change the timezone for your session, which will affect how
-- TIMESTAMP values are displayed. The underlying data is stored in UTC.
ALTER SESSION SET TIMEZONE = 'America/Los_Angeles';
SELECT * FROM my_ts_table;

ALTER SESSION SET TIMEZONE = 'Japan';
SELECT * FROM my_ts_table;


-- /////////////////////////////////////////////////////////////////////////////
-- Section 3: Object Identifiers (Case Sensitivity and Special Characters)
-- /////////////////////////////////////////////////////////////////////////////

-- To preserve case or use spaces/special characters, you MUST use double quotes.
-- When an identifier is double-quoted, it becomes case-sensitive.

CREATE TABLE "my table" (my_field STRING);
CREATE TABLE "My Table" ("my field" STRING, "My Field" STRING);

-- SHOW TABLES will now display all tables, including the case-sensitive ones.
SHOW TABLES;

-- You must use double quotes to describe or query these specific tables.
DESC TABLE "my table";
DESC TABLE "My Table";


-- /////////////////////////////////////////////////////////////////////////////
-- Section 4: Constraints (Primary Key, Not Null, Unique)
-- /////////////////////////////////////////////////////////////////////////////

-- IMPORTANT: Snowflake supports defining constraints for data modeling and
-- compatibility, but it ONLY ENFORCES the `NOT NULL` constraint.
-- `PRIMARY KEY` and `UNIQUE` constraints are NOT enforced.

CREATE TABLE my_constraints_table (
    emp_pk STRING PRIMARY KEY, -- Defines a PK, but it is not enforced
    fname STRING NOT NULL,     -- This constraint IS enforced
    lname STRING NOT NULL,     -- This constraint IS enforced
    flag STRING DEFAULT 'active',
    unique_code STRING UNIQUE  -- Defines a UNIQUE constraint, but it is not enforced
);

-- This insert will SUCCEED, even though we are inserting duplicate values
-- for the defined PRIMARY KEY and UNIQUE columns.
INSERT INTO my_constraints_table (emp_pk, fname, lname, unique_code) VALUES
    ('100', 'John1', 'K', '1000'),
    ('100', 'John2', 'K', '1000');

SELECT * FROM my_constraints_table;

-- This insert will FAIL because the 'lname' column is defined as NOT NULL
-- and no value is provided for it.
-- INSERT INTO my_constraints_table (emp_pk, fname, unique_code) VALUES ('100', 'John4', '1000');


-- /////////////////////////////////////////////////////////////////////////////
-- Section 5: Data Loading and Table Management
-- /////////////////////////////////////////////////////////////////////////////

-- == Subsection 5.1: Create Table As Select (CTAS) ==
-- A convenient way to create and populate a table from a query result.
CREATE TABLE my_ctas_big_table AS SELECT * FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF100"."ORDERS";

-- Check the first 10 rows of the newly created table.
SELECT * FROM my_ctas_big_table LIMIT 10;


-- == Subsection 5.2: Loading from an Internal Stage ==
-- To load data from a local file, you must first create an internal stage,
-- then use the PUT command (from SnowSQL CLI) to upload the file to that stage,
-- and finally use the COPY command to load the data into a table.

-- 1. Create an internal named stage (can also be done via UI).
CREATE STAGE IF NOT EXISTS my_stg;

-- 2. (Run from SnowSQL CLI) Upload the local file to the stage.
--    put file:///tmp/ch07.csv @my_stg;

-- 3. (Run from Snowflake Worksheet) List the files in the stage to confirm upload.
LIST @my_stg;

-- 4. Create a target table for the data.
CREATE TABLE my_stg_table (
    num NUMBER,
    num10_1 NUMBER(10,1),
    decimal_20_2 DECIMAL(20,2),
    numeric_field NUMERIC(30,3),
    int_field INT,
    integer_field INTEGER
);

-- 5. Use the COPY command to load the data from the stage into the table.
COPY INTO my_stg_table FROM @my_stg;

-- 6. Verify the data has been loaded.
SELECT * FROM my_stg_table;


-- == Subsection 5.3: Loading and Transforming Semi-Structured Data (JSON) ==
-- Create a table with a single VARIANT column to ingest the raw JSON.
CREATE TABLE json_weather_data (v VARIANT);

-- Create an external stage pointing to a public S3 bucket containing JSON files.
CREATE STAGE nyc_weather url = 's3://toppertips-workshop-lab/';

-- Load the JSON files directly into the VARIANT table.
COPY INTO json_weather_data
FROM @nyc_weather
FILE_FORMAT = (TYPE = JSON);

-- Create a view to flatten the JSON. This parses the VARIANT data into a
-- relational, columnar format on-the-fly.
CREATE OR REPLACE VIEW json_weather_data_view AS
SELECT
    v:time::TIMESTAMP AS observation_time,
    v:city.id::INT AS city_id,
    v:city.name::STRING AS city_name,
    v:city.country::STRING AS country
FROM json_weather_data;

-- Now you can query the view just like a regular table.
SELECT * FROM json_weather_data_view LIMIT 10;


-- /////////////////////////////////////////////////////////////////////////////
-- Section 6: Time Travel
-- /////////////////////////////////////////////////////////////////////////////

-- Time Travel allows you to query data as it existed in the past.
-- First, let's get the distribution of the O_ORDERSTATUS column.
SELECT O_ORDERSTATUS, COUNT(1) FROM my_ctas_big_table GROUP BY O_ORDERSTATUS;

-- Now, run an UPDATE statement. This will create a new micro-partition for
-- the changed data, but the old micro-partitions are retained for Time Travel.
UPDATE my_ctas_big_table SET O_ORDERSTATUS = 'p' WHERE O_ORDERSTATUS = 'P';

-- The query ID for the UPDATE statement is needed for Time Travel.
-- Copy the Query ID from the History tab after running the UPDATE.
-- Example Query ID: '019f0815-0c81-d7d2-0000-0001acbe9e89'

-- This query uses Time Travel to check the state of the table BEFORE the UPDATE.
-- It will return the count of rows where the status was 'P'.
-- The current state of the table has no rows with status 'P'.
SELECT COUNT(*) FROM my_ctas_big_table BEFORE (STATEMENT => 'PASTE_YOUR_UPDATE_QUERY_ID_HERE')
WHERE O_ORDERSTATUS = 'P';