-- create a secure share 
CREATE SHARE my_share;
GRANT USAGE ON DATABASE my_db TO SHARE my_share;
GRANT SELECT ON SCHEMA my_db.public TO SHARE my_share;
GRANT SELECT ON TABLE my_db.public.my_table TO SHARE my_share;


-- create a user account 

CREATE MANAGED ACCOUNT my_reader_account
ADMIN_NAME = reader_admin
ADMIN_PASSWORD = 'StrongPassword123!'
EMAIL = 'reader@example.com'
COMMENT = 'Reader account for external client';


-- associate the share with user account 
ALTER SHARE my_share ADD ACCOUNTS = my_reader_account;

-- provide access instructions to reader 

CREATE DATABASE shared_db FROM SHARE your_provider_account.my_share;
