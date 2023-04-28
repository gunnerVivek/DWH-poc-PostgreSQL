
-- ##########################################
--  	 Create the Stage Schema
-- ##########################################

CREATE SCHEMA IF NOT EXISTS sales_mart_stage;


-- ##########################################
--  	 Create the Intermediate Tables
-- ##########################################

-- SCD 2
DROP TABLE IF EXISTS sales_mart_stage.dim_customer_stage ;
--TRUNCATE TABLE IF EXISTS sales_mart_stage.dim_customer_stage ;

CREATE TABLE IF NOT EXISTS sales_mart_stage.dim_customer_stage(
	customer_id varchar(100) 
	, account_status int  --CHECK (account_status IN (1,0))
	, customer_type varchar(1)  --CHECK(customer_type IN ('S','P', 'M'))-- Subscriber, Purchaser or MEMBER
	, "name" varchar(100) 
	, gender varchar(1) --CHECK(gender IN ('M', 'F', 'O'))
	, email TEXT 
	, dob date
	, phone_number varchar(20)  
	, address TEXT 
	, city varchar(50)  
	, state varchar(50) 
	, country varchar(50) 
	, zipcode varchar(20) 
	, created_time timestamp  -- time record was created AT SOURCE FOR the 1st time
	, update_time timestamp  -- time record was modified, loaded INTO the DWH
	, effective_date timestamp  -- only target
	, end_date timestamp  -- only target
	, is_active int  --CHECK(is_active IN (0,1)) Only in Target
	, hash_key TEXT 
)
;

-- SCD 1
DROP TABLE IF EXISTS sales_mart_stage.dim_customer_profile_stage ;

CREATE TABLE IF NOT EXISTS sales_mart_stage.dim_customer_profile_stage(
	customer_id varchar(100) 
	, account_status int  --CHECK (account_status IN (1,0))
	, customer_class int --CHECK(customer_class IN (1,2,3,4,5))
	, customer_band varchar(1) --CHECK (customer_band IN ('A', 'B', 'C', 'D', 'E'))
	, credit_score int --CHECK(credit_score >= 0 AND credit_score <= 800)
	, income_level int --CHECK(income_level IN (1,2,3,4,5))
	, customer_occupation_category varchar(20) 
		--CHECK (customer_occupation_category IN ('Retired', 'Job', 'Business', 'Social', 'Student'))
)
;


-- SCD 3
DROP TABLE IF EXISTS sales_mart_stage.dim_product_stage ;

CREATE TABLE IF NOT EXISTS sales_mart_stage.dim_product_stage(
	product_id varchar(20) 
	, sku varchar(50) 
	, "name" varchar(20) 
	, product_description TEXT
	, package_type varchar(20)
 	, product_category varchar(20)
	, product_size varchar(20)
	, product_unit_size_desc varchar(30) -- {singles pack, doubles pack, etc}
	, package_weight int
	, package_weight_unit varchar(5) -- CHECK(package_weight_unit IN ('gm', 'kg', 'mg'))
	, package_color_code int --{colors described IN code}
	, brand_name varchar(50)
	, price NUMERIC(9,2) 
	, last_price NUMERIC(9,2)
	, status int --CHECK(status IN (0,1))-- {1: Active| 0:Expired}
	, created_time timestamp  -- to deal WITH back-fill issue
	, update_time timestamp  -- to deal WITH back-fill issue
	, hash_key TEXT 
) 
;


DROP TABLE IF EXISTS sales_mart_stage.fact_sales ;

CREATE TABLE IF NOT EXISTS sales_mart_stage.fact_sales(
	sales_line_number int GENERATED ALWAYS AS IDENTITY -- {Surrogate; autoincrementing}
	, sales_date_key int  -- refernce dim_date
	, customer_id varchar(100) -- reference dim_cutomer AND dim_customer_profile
	, product_id varchar(20)
	, product_sku varchar(50)
	, order_id varchar(30)
	, unit_price NUMERIC(9,2) 
	, unit_cost NUMERIC(9,2)
	, quantity int
	, sales_value NUMERIC(9,2) -- quantity * unit_price
	, sales_cost NUMERIC(9,2)  -- quantity * unit_cost
	, margin NUMERIC(9,2) -- sales_value - sales_cost
	, sales_timestamp timestamp
	, created_time timestamp 
	, update_time timestamp 
)
;

COMMIT ;
