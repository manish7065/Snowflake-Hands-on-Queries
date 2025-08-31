-- creating a udf to calculate the square of a number 
CREATE OR REPLACE FUNCTION square_number(num  FLOAT)
RETURNS FLOAT
LANGUAGE SQL
AS
$$
    num * num
$$;

-- Testing the udf
SELECT square_number(5.036);


-- creating a function for cube a number

CREATE OR  REPLACE FUNCTION cube_number(num FLOAT)
RETURNS  FLOAT
LANGUAGE SQL
AS
$$
    num * num * num
$$;

-- Test Case
select cube_number(5), cube_number(58), cube_number(-6);


-- creating a javascript UDF to check even number
CREATE OR REPLaCE FUNCTION is_even(num INTEGER)
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
AS
$$
  return num % 2 === 0;
$$;
-- ABOVE IS GIVING ERROR BECAUSE JAVASCRIPT DOSENT SUPPORT INTEGER TYPE

--- JS function to categorise cold  moderate or hot as per the given temperature

CREATE OR REPLACE FUNCTION temperature_category(temp FLOAT)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  if (TEMP < 10) return 'Cold';
  else if (TEMP >= 10 && TEMP <= 25) return 'Moderate';
  else return 'Hot';
$$;

-- Test the UDF
SELECT temperature_category(5), temperature_category(20), temperature_category(30);
-- Expected output: 'Cold', 'Moderate', 'Hot'


CREATE OR REPLACE FUNCTION is_even(num FLOAT)
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
AS
$$
  return NUM % 2 === 0;
$$;

--- TEST UDF
SELECT is_even(8.2);




-- Grading Function

CREATE OR REPLACE FUNCTION grade_score(score float)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  if (SCORE >= 90) return 'A';
  else if (SCORE >= 80) return 'B';
  else if (SCORE >= 70) return 'C';
  else if (SCORE >= 60) return 'D';
  else return 'F';
$$;

-- Test cases
SELECT grade_score(95), grade_score(85), grade_score(75), grade_score(65), grade_score(55);

--- creating and testing USER DEFINED TABLE FUNTIONS (UDTFs)

CREATE OR REPLACE FUNCTION generate_sequence(start_ float, end_ float)
RETURNS TABLE (number float)
LANGUAGE SQL
AS
$$
  SELECT seq4() + start_ AS number
  FROM TABLE(GENERATOR(rowcount => end_ - start_ + 1))
$$;

-- Test the UDTF
SELECT number FROM TABLE(generate_sequence(1, 5));
-- Expected output: 1, 2, 3, 4, 5


CREATE OR REPLACE FUNCTION safe_sqrt(num FLOAT)
RETURNS FLOAT
LANGUAGE JAVASCRIPT
AS
$$
  if (NUM < 0) return null;
  return Math.sqrt(NUM);
$$;

-- Test cases
SELECT safe_sqrt(16), safe_sqrt(0), safe_sqrt(-4);


--- for more complex UDF use javascript or pthon for better performance
-- factorial function

CREATE OR REPLACE FUNCTION factorial(n float)
RETURNS float
LANGUAGE JAVASCRIPT
AS
$$
  if (N < 0) return null;
  if (N === 0 || N === 1) return 1;
  let result = 1;
  for (let i = 2; i <= N; i++) {
    result *= i;
  }
  return result;
$$;

-- Test cases
SELECT factorial(5), factorial(3), factorial(0);


CREATE OR REPLACE VIEW discounted_prices AS
SELECT product_id, price, calculate_discount(price, 10) AS discounted_price
FROM products;

-- Query the view
SELECT * FROM discounted_prices;


-- using UDF in Stored Procedure

CREATE OR REPLACE PROCEDURE process_data()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  var rs = snowflake.execute({sqlText: "SELECT square_number(5) AS result"});
  rs.next();
  return "Result: " + rs.getColumnValue(1);
$$;

-- Call the procedure
CALL process_data(); -- Expected output: Result: 25

select square_number(4);