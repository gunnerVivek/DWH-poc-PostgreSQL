-- ##############################################
--          Load data into load Tables
-- If there are permission issues to reda data fromm file;
-- use \copy command from pslq shell		
-- ##############################################


-- 1. Load the Customer Load Table 
COPY load_schema.customer_load(
	customer_id, account_status, customer_type, "name", gender, email, dob  
	, phone_number, address--, city, state, country, zipcode
	, customer_class, customer_band, credit_score, income_level
	, customer_occupation_category, created_time
) 
FROM 
'/home/vivek/User_Data/repositories/dwh-poc-postgresql/data/customer_load.csv'
DELIMITER ',' CSV HEADER
;

-- Update the hash Key; If we do during Load using Always Generated AS then it 
-- will take lot of time as md5 / sha256 are costly functions

-- 1. set a row number Surrogate key to Load table - temporarily
ALTER TABLE load_schema.customer_load  
ADD COLUMN row_num int GENERATED ALWAYS AS IDENTITY
;

-- 2. Calculate hash key for each row number and upadte the load table has key
--    use CTE to calculate hash_key, select 2 columns row_num and hask_key
--    update the Load table hash key column on row_num
WITH hash_calculated AS 
(
	SELECT 
		row_num
		, md5(
			customer_id|| account_status|| customer_type|| "name" || gender
			|| email|| dob || phone_number
			|| split_part(address, '|', 2) -- city
			|| split_part(address, '|', 3) -- state
			|| split_part(address, '|', 4) -- country
			|| split_part(address, '|', 5) -- zipcode
			)::uuid AS hash_key_dim_customer
	FROM load_schema.customer_load AS cl 
)
UPDATE load_schema.customer_load 
SET  hash_key_dim_customer = hash_calculated.hash_key_dim_customer
FROM hash_calculated
WHERE load_schema.customer_load.row_num = hash_calculated.row_num
;

--3 Drop the row_num column
ALTER TABLE load_schema.customer_load 
DROP COLUMN row_num
;



-- 2. Load the Product Load Table
COPY load_schema.product_load(
	product_id, sku, "name", product_description, package_type, product_category
	, product_size, product_unit_size_desc, package_weight, package_weight_unit
	, package_color_code, brand_name, price, last_price, status, created_time
)
FROM '/home/vivek/User_Data/scd_implementation_poc/data/product_load.csv'
DELIMITER ',' CSV HEADER
;


-- 3. Load the Sales Load Table
COPY load_schema.sales_load(

	sales_date_key, customer_id, product_id, product_sku, order_id, unit_price 
	, unit_cost, quantity, sales_timestamp, created_time
)
FROM '/home/vivek/User_Data/scd_implementation_poc/data/sales_load.csv'
DELIMITER ',' CSV HEADER
;
