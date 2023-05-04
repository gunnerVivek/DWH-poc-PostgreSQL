
-- ##########################################
--       Audit Tables & audit Log table     
-- ##########################################
CREATE SCHEMA IF NOT EXISTS load_schema;



DROP TABLE IF EXISTS load_schema.customer_audit ;


CREATE TABLE load_schema.customer_audit(
	customer_id varchar(100) 
	, account_status int  --CHECK (account_status IN (1,0))
	, customer_type varchar(1)  --CHECK(customer_type IN ('S','P', 'M'))-- Subscriber, Purchaser or MEMBER
	, "name" varchar(100) 
	, gender varchar(1) --CHECK(gender IN ('M', 'F', 'O'))
	, email TEXT 
	, dob date  
	, phone_number varchar(20)  
	, address TEXT 
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
	, CONSTRAINT load_schema_customer_audit_pkey PRIMARY KEY (customer_id)
) ;


DROP TABLE IF EXISTS load_schema.product_audit ;

CREATE TABLE load_schema.product_audit(
	product_id varchar(20) 
	, sku varchar(50) 
	, "name" varchar(20) 
	, product_description TEXT
	, package_type varchar(20)
 	, product_category varchar(20)
	, product_size varchar(20)
	, product_unit_size_desc varchar(30) -- {singles pack, doubles pack, etc}
	, package_weight int
	, package_weight_unit varchar(5) CHECK(package_weight_unit IN ('gm', 'kg', 'mg'))
	, package_color_code int --{colors described IN code}
	, brand_name varchar(50)
	, price NUMERIC(9,2) 
	, last_price NUMERIC(9,2)
	, status int CHECK(status IN (0,1))-- {1: Active| 0:Expired}
	, created_time timestamp  -- to deal WITH back-fill issue
	, CONSTRAINT load_schema_product_audit_pkey PRIMARY KEY (product_id, sku)
) ;


DROP TABLE IF EXISTS load_schema.sales_audit ;

CREATE TABLE load_schema.sales_audit(
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
	, hash_key uuid 
	, CONSTRAINT load_schema_sales_audit_pkey PRIMARY KEY (hash_key)
) ;


COMMIT ;

-- ##############################
-- 			Audit Log
-- ##############################

DROP TABLE IF EXISTS load_schema.audit_logs ;

CREATE TABLE load_schema.audit_logs(
	record_id varchar(100)
	, table_name varchar(50)
	, audit_rule_id varchar(50)
	, description text
) ;

COMMIT ;


-- ##############################################
--          Load data into load Tables
-- If there are permission issues to reda data fromm file;
-- use \copy command from pslq shell		
-- ##############################################

COPY FROM load_schema.customer_load(
	customer_id, account_status, customer_type, "name", gender, email, dob  
	, phone_number, address--, city, state, country, zipcode
	, customer_class, customer_band, credit_score, income_level
	, customer_occupation_category, created_time
) 
FROM 
'/home/vivek/User_Data/repositories/dwh-poc-postgresql/data/customer_load.csv'
DELIMITER ',' CSV HEADER
;


COPY load_schema.product_load(
	product_id, sku, "name", product_description, package_type, product_category
	, product_size, product_unit_size_desc, package_weight, package_weight_unit
	, package_color_code, brand_name, price, last_price, status, created_time
)
FROM '/home/vivek/User_Data/scd_implementation_poc/data/product_load.csv'
DELIMITER ',' CSV HEADER
;


COPY load_schema.sales_load(

	sales_date_key, customer_id, product_id, product_sku, order_id, unit_price 
	, unit_cost, quantity, sales_timestamp, created_time
)
FROM '/home/vivek/User_Data/scd_implementation_poc/data/sales_load.csv'
DELIMITER ',' CSV HEADER
;


-- ##############################################
-- 		Audit Rules for Customers Load Table
-- ##############################################
--audit_rule_id varchar(50)
--, audit_rule_name varchar(50)
--, description TEXT
--, CONSTRAINT audit_rule_pk PRIMARY KEY (audit_rule_id)

-- rule_id, rule_name, description

--TRUNCATE load_schema.audit_rules;

INSERT INTO foreign_schema.audit_rules VALUES
('check_cust_01', 'customer_load_account_status_values'
	, 'Check account_status column has value either 1 or 0. 1: Active, 0: In-active.'
	, 'account_status NOT IN (1,0)'
)
, ('check_cust_02', 'customer_load_customer_type_values'
	, 'Check customer_type column has value in S, P or M. S:Subscriber, P:Purchaser, M:MEMBER'
	, $$customer_type NOT IN ('S','P','M')$$
)
, ('check_cust_03', 'customer_load_gender_values'
	, 'Check gender column has values in M: Male, F: Female or O:Other'
	, $$gender NOT IN ('M', 'F', 'O')$$
)
, ('check_cust_04', 'customer_load_customer_class_values'
	, 'Check customer_class column has values in (1,2,3,4,5)'
	, $$customer_class NOT IN (1,2,3,4,5)$$
)
, ('check_cust_05', 'customer_load_customer_band_values'
	, 'Check customer_band column has values in (A, B, C, D, E)'
	, $$customer_band NOT IN ('A', 'B', 'C', 'D', 'E')$$
)
, ('check_cust_06', 'customer_load_credit_score_values'
	, 'Check credit_score column has values >= 0 and <= 800'
	, $$(credit_score NOT BETWEEN 0 AND 800)$$
) -- credit_score should fall in the range of 0 to 800. Both inclusive.  
, ('check_cust_07', 'customer_load_income_level_values'
	, 'Check income_level column has values in (1,2,3,4,5)'
	, $$income_level NOT IN (1,2,3,4,5)$$
)
, ('check_cust_08', 'customer_load_not_null'
	, 'NULL value is not allowed in any of the following columns in customer_load table: '
		|| 'customer_id, account_status, customer_type, email, dob, phone_number, address, city, state
			, country, zip_code or created_time'
	, $$customer_id IS NULL OR account_status IS NULL OR customer_type IS NULL OR email IS NULL OR 
		dob IS NULL OR phone_number IS NULL OR address IS NULL OR created_time IS NULL
	$$
)
, ('drop_duplicate_cust', 'customer_load_drop_duplicate', 'Drop the duplicates for the Customer Load Table', Null)
, ('unique_cust_01', 'customer_load_email_unique', 'Every email should be unique to a customer', Null)
;
-- OR city IS NULL OR state IS NULL OR
--		country IS NULL OR zip_code IS NULL 

-- ##############################################
-- 		Audit Rules for Product Load Table
-- ##############################################

INSERT INTO foreign_schema.audit_rules VALUES
('check_prod_01', 'product_load_package_weight_unit_check', 'Check package_weight_unit column has vlues IN (gm, kg, mg)'
	, $$package_weight_unit NOT IN ('gm','kg','mg')$$
)
, ('check_prod_02', 'product_load_status_check', 'Check status column has vlaues IN (0,1)'
	, $$status NOT IN (0,1)$$
)
, ('check_prod_03', 'product_load_not_null'
		, 'NULL value is not allowed in any of the following columns in customer_load table: '
		|| 'product_id, sku, name, price or created_time'
		, $$product_id IS NULL OR sku IS NULL OR name IS NULL OR price IS NULL OR created_time IS NULL$$
)
, ('drop_duplicate_prod', 'product_load_drop_duplicate', 'Drop duplicate records in Product Load table', Null)
, ('unique_prod_01', 'product_load_sku_unique', 'SKU must be unique for each Product Id.', Null)
;


-- ##############################################
-- 		Audit Rules for Sales Load Table
-- ##############################################

INSERT INTO foreign_schema.audit_rules VALUES
('check_sales_01', 'sales_load_not_null'
	, 'NULL value is not allowed in any of the following columns in sales_load table: '
	|| 'sales_date_key, customer_id '
	, $$sales_date_key IS NULL OR customer_id IS NULL$$
)
, ('drop_duplicate_sales', 'sales_load_drop_duplicate', 'Drop duplicate columns for the Sales Load Table', Null)
;

