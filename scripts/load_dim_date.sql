-- ##########################################
--  		Create the Schemas
-- ##########################################
CREATE SCHEMA IF NOT EXISTS common_dims;


-- ##########################################
-- 			  Load Date dimension
-- ##########################################
DROP TABLE IF EXISTS common_dims.dim_date ;

CREATE TABLE common_dims.dim_date (
	date_key int NOT NULL 
	, "date" date NOT NULL
	, day_name varchar(9) NOT NULL
	, day_name_short varchar(3) NOT NULL
	, "month" int NOT NULL
	, month_name varchar(9) NOT NULL
	, month_name_short varchar(3) NOT NULL
	, "quarter" int NOT NULL
	, "year" int NOT NULL
	, day_of_week int NOT NULL
	, day_of_month int NOT NULL
	, day_of_quarter int NOT NULL
	, day_of_year int NOT NULL
	, week_of_month int NOT NULL
	, week_of_year int NOT NULL
	, first_day_of_week date NOT NULL
	, last_day_of_week date NOT NULL
	, first_day_of_month date NOT NULL
	, last_day_of_month date NOT NULL
	, first_day_of_quarter date NOT NULL
	, last_day_of_quarter date NOT NULL
	, first_day_of_year date NOT NULL
	, last_day_of_year date NOT NULL
	, is_weekend boolean NOT null
	, CONSTRAINT dim_date_pkey_date_key PRIMARY KEY (date_key)
);

COMMIT;

WITH date_bounds(start_date, end_date) AS -- BOTH dates are inclusive  
(
	Values('2023-01-01', '2023-12-31')
)
, date_series AS
( -- generates a series OF dates BETWEEN two given dates - start_date & end_date, both inclusive.
	SELECT 
		date_series::date AS "date_actual"
	FROM
		generate_series((SELECT start_date FROM date_bounds) :: timestamp WITHOUT time ZONE
			, (SELECT end_date FROM date_bounds) :: timestamp WITHOUT time ZONE
			, INTERVAL '1 day'
		) ds(date_series) 
)
INSERT INTO common_dims.dim_date
SELECT
	date_actual AS "date"
	, to_char(date_actual, 'yyyymmdd') :: Int AS date_key
	, to_char(date_actual, 'TMDay') AS day_name
	, to_char(date_actual, 'MM') :: Int AS "month"
	, to_char(date_actual, 'Month') AS month_name
	, to_char(date_actual, 'Mon') AS month_name_short
	, to_char(date_actual, 'q') :: Int AS "quarter"
	, to_char(date_actual, 'YYYY') :: Int AS "year"
	, to_char(date_actual, 'ID') :: Int AS day_of_week -- Monday -> 1
	, to_char(date_actual, 'DD') AS day_of_month
	, (date_actual - DATE_TRUNC('quarter', date_actual)::DATE + 1) :: Int AS day_of_quarter
	, to_char(date_actual, 'DDD') :: Int AS day_of_year
	, to_char(date_actual, 'W') :: Int AS week_of_month
	, to_char(date_actual, 'WW') :: Int AS week_of_year
	, date_actual + (1 - EXTRACT(ISODOW FROM date_actual)::Int) :: date AS first_day_of_week
	, date_actual + (1 - EXTRACT(ISODOW FROM date_actual)::Int) :: date AS last_day_of_week
	, date_actual + (1 - EXTRACT(DAY FROM date_actual)::Int) :: date AS first_day_of_month
	, (DATE_TRUNC('MONTH', date_actual) + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month
	, DATE_TRUNC('quarter', date_actual)::DATE AS first_day_of_quarter
	, (DATE_TRUNC('quarter', date_actual) + INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter
	, TO_DATE(EXTRACT(YEAR FROM date_actual) || '-01-01', 'YYYY-MM-DD') AS first_day_of_year
	, TO_DATE(EXTRACT(YEAR FROM date_actual) || '-12-31', 'YYYY-MM-DD') AS last_day_of_year
	, CASE
           WHEN EXTRACT(ISODOW FROM date_actual) IN (6, 7) THEN TRUE
           ELSE FALSE
       END AS is_weekend
	
FROM 
	date_series
;

COMMIT;


