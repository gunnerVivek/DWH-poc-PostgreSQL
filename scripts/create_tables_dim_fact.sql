
-- ##########################################
--  		Create the Schemas
-- ##########################################

CREATE SCHEMA IF NOT EXISTS sales_mart;


-- ##########################################
--  		Create the Target tables
-- ##########################################

-- SCD Type 1
-- account_status is not a profile information but is stored for analytical reporting purpose
CREATE TABLE IF NOT EXISTS sales_mart.dim_customer_profile(
	customer_id varchar(100) NOT NULL
	, account_status int NOT NULL --CHECK (account_status IN (1,0))
	, customer_class int --CHECK(customer_class IN (1,2,3,4,5))
	, customer_band varchar(1) --CHECK (customer_band IN ('A', 'B', 'C', 'D', 'E'))
	, credit_score int --CHECK(credit_score >= 0 AND credit_score <= 800)
	, income_level int --CHECK(income_level IN (1,2,3,4,5))
	, customer_occupation_category varchar(20) 
		--CHECK (customer_occupation_category IN ('Retired', 'Job', 'Business', 'Social', 'Student'))
	, PRIMARY KEY (customer_id)
) 
;


-- SCD 2
CREATE TABLE IF NOT EXISTS sales_mart.dim_customer(
	customer_id varchar(100) NOT NULL
	, account_status int NOT NULL --CHECK (account_status IN (1,0))
	, customer_type varchar(1) NOT NULL --CHECK(customer_type IN ('S','P', 'M'))-- Subscriber, Purchaser or MEMBER
	, "name" varchar(100) 
	, gender varchar(1) --CHECK(gender IN ('M', 'F', 'O'))
	, email TEXT UNIQUE NOT NULL
	, dob date NOT NULL 
	, phone_number varchar(20) NOT NULL 
	, address TEXT NOT NULL
	, city varchar(50) NOT NULL 
	, state varchar(50) NOT NULL
	, country varchar(50) NOT NULL
	, zipcode varchar(20) NOT NULL
	, created_time timestamp NOT NULL -- time record was created AT SOURCE FOR the 1st time
	, update_time timestamp NOT NULL -- time record was modified, loaded INTO the DWH
	, effective_start_date timestamp NOT NULL
	, effective_end_date timestamp NOT NULL
	, is_active int NOT NULL --CHECK(is_active IN (0,1))
	, hash_key TEXT NOT NULL
	, PRIMARY KEY (customer_id)
)
;

-- ## Product Dim ##
-- SCD 3: SCD 1 is enough but for compliance using 3
-- track change only in price
-- Hash key will contain all the attributes for which we want to capture change, including primary key;
-- primary key is included to maintain uniqueness and avoid hash collision
CREATE TABLE IF NOT EXISTS sales_mart.dim_product(
	product_id varchar(20) NOT NULL
	, sku varchar(50) NOT NULL
	, "name" varchar(20) NOT NULL
	, product_description TEXT
	, package_type varchar(20)
 	, product_category varchar(20)
	, product_size varchar(20)
	, product_unit_size_desc varchar(30) -- {singles pack, doubles pack, etc}
	, package_weight int
	, package_weight_unit varchar(5) -- CHECK(package_weight_unit IN ('gm', 'kg', 'mg'))
	, package_color_code int --{colors described IN code}
	, brand_name varchar(50)
	, price NUMERIC(9,2) NOT NULL
	, last_price NUMERIC(9,2)
	, status int --CHECK(status IN (0,1))-- {1: Active| 0:Expired}
	, created_time timestamp NOT NULL -- to deal WITH back-fill issue
	, update_time timestamp NOT NULL -- to deal WITH back-fill issue
	, hash_key TEXT NOT NULL
	, PRIMARY KEY (product_id, sku)
)
;

-- Sales Fact
-- SCD 0
-- create foreign key references
CREATE TABLE IF NOT EXISTS sales_mart.fact_sales(
	sales_line_number int GENERATED ALWAYS AS IDENTITY -- {Surrogate; autoincrementing}
	, sales_date_key int SET NOT NULL -- refernce dim_date
	, customer_id varchar(100) SET NOT NULL -- reference dim_cutomer AND dim_customer_profile
	, product_id varchar(20) SET NOT NULL
	, product_sku varchar(50) SET NOT NULL
	, order_id varchar(30) SET NOT NULL
	, unit_price NUMERIC(9,2) SET NOT NULL
	, unit_cost NUMERIC(9,2) SET NOT NULL
	, quantity int SET NOT NULL
	, sales_value NUMERIC(9,2) SET NOT NULL -- quantity * unit_price
	, sales_cost NUMERIC(9,2)  SET NOT NULL -- quantity * unit_cost
	, margin NUMERIC(9,2) SET NOT NULL -- sales_value - sales_cost
	, sales_timestamp timestamp SET NOT NULL
	, created_time timestamp NOT NULL
	, update_time timestamp NOT NULL
	, PRIMARY KEY (sales_line_number)
)
;


COMMIT;
