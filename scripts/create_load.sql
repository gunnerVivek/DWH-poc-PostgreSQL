-- #####################################################################
--  	                   Create the LOAD Tables
--   Wont have Primary Keys because all these checks are built into QA
--	 AND also we want faster load
-- #####################################################################
DROP FUNCTION md5_immutable(text);
CREATE OR REPLACE FUNCTION md5_immutable(text) RETURNS uuid IMMUTABLE 
LANGUAGE SQL
AS 
$Body$
	SELECT md5($1)::uuid; 
$Body$
;




CREATE SCHEMA IF NOT EXISTS load_schema; 


DROP TABLE IF EXISTS load_schema.customer_load ;

CREATE TABLE  load_schema.customer_load (
	customer_id varchar(100) 
	, account_status int  --CHECK (account_status IN (1,0))
	, customer_type varchar(1)  --CHECK(customer_type IN ('S','P', 'M'))-- Subscriber, Purchaser or MEMBER
	, "name" varchar(100) 
	, gender varchar(1) --CHECK(gender IN ('M', 'F', 'O'))
	, email TEXT 
	, dob date  
	, phone_number varchar(20)  
	, address TEXT -- ALL the below parts will be present IN this field comma separated
--	, city varchar(50)  
--	, state varchar(50) 
--	, country varchar(50) 
--	, zipcode varchar(20) 
	, customer_class int --CHECK(customer_class IN (1,2,3,4,5))
	, customer_band varchar(1) --CHECK (customer_band IN ('A', 'B', 'C', 'D', 'E'))
	, credit_score int --CHECK(credit_score >= 0 AND credit_score <= 800)
	, income_level int --CHECK(income_level IN (1,2,3,4,5))
	, customer_occupation_category varchar(20)
	, created_time timestamp  -- time record was created AT SOURCE FOR the 1st time
)  
;

-- Add a hash key for the customer dimensions SCD columns  
ALTER TABLE load_schema.customer_load 
ADD COLUMN hash_key_dim_customer uuid ; 
--GENERATED ALWAYS AS (
--		sha256(
--			customer_id|| account_status|| customer_type|| "name" || gender
--			|| email|| dob || phone_number
--			|| split_part(address, '|', 2) -- city
--			|| split_part(address, '|', 3) -- state
--			|| split_part(address, '|', 4) -- country
--			|| split_part(address, '|', 5) -- zipcode
--			)
--		::uuid)  STORED
;



DROP TABLE IF EXISTS load_schema.product_load ;
-- No hash key because SCD 3
CREATE TABLE  load_schema.product_load (
	product_id varchar(20) 
	, sku varchar(50) 
	, "name" varchar(20) 
	, product_description TEXT
	, package_type varchar(20)
 	, product_category varchar(20)
	, product_size varchar(20)
	, product_unit_size_desc varchar(30) -- {singles pack, doubles pack, etc}
	, package_weight int
	, package_weight_unit varchar(5) --CHECK(package_weight_unit IN ('gm', 'kg', 'mg'))
	, package_color_code int --{colors described IN code}
	, brand_name varchar(50)
	, price NUMERIC(9,2) 
	, last_price NUMERIC(9,2)
	, status int --CHECK(status IN (0,1))-- {1: Active| 0:Expired}
	, created_time timestamp  -- to deal WITH back-fill issue
);


DROP TABLE IF EXISTS load_schema.sales_load;

-- Adding hash_key because it will be used to make a single entry in the 
-- Audit table in case the record fails multiple QA rules.
CREATE TABLE  load_schema.sales_load(
	sales_date_key int  -- refernce dim_date
	, customer_id varchar(100) -- reference dim_cutomer AND dim_customer_profile
	, product_id varchar(20)
	, product_sku varchar(50)
	, order_id varchar(30)
	, unit_price NUMERIC(9,2) 
	, unit_cost NUMERIC(9,2)
	, quantity int
	, sales_timestamp timestamp
	, created_time timestamp
	, hash_key uuid GENERATED ALWAYS AS (md5(product_id::varchar(20) || product_sku)::uuid)  STORED
		-- hash_key is only for use as PK in audit Table
) 
;








DROP TABLE load_schema.tmp_tbl;

CREATE  TABLE load_schema.tmp_tbl(
	product_id varchar(20)
	, sku varchar(50)
	, price numeric(6,2)
--	, h_key uuid
	--, primary_key varchar(82) GENERATED ALWAYS AS (product_id::varchar(20) || '+' || sku)  STORED PRIMARY KEY
--	, primary_key varchar(82) GENERATED ALWAYS AS (md5(concat(product_id::varchar(20),sku))::uuid)  STORED PRIMARY KEY
--	, primary_key uuid DEFAULT gen_random_uuid()
	--, primary_key uuid GENERATED ALWAYS AS (md5(product_id::varchar(20) || sku)::uuid)  STORED  
) ;

INSERT INTO load_schema.tmp_tbl VALUES (
	'p_01', 'sku_01', 21
);

SELECT * from load_schema.tmp_tbl;

-- SELECT hash_record_extended(tbl.*, 0) FROM load_schema.tmp_tbl AS tbl LIMIT 1;

UPDATE load_schema.tmp_tbl 
SET h_key = md5(product_id || sku)::uuid;

SELECT * from load_schema.tmp_tbl;

SELECT gen_random_uuid();
