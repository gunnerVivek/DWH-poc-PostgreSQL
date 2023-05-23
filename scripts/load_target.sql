-- #######################################
--        Load All the Stage Tables
-- #######################################


-- #########  Dim Customer Table #############
-- how to support BACKFILL ?

-- 1) Load expired from TGT to Stage;
-- All records with Non - null end date and active flag = 0  
-- >> TGT to Stage

INSERT INTO sales_mart_stage.dim_customer_stage 
SELECT *
FROM 
sales_mart.dim_customer AS dc
WHERE -- records that ARE NOT active
	is_active = 0 
	AND effective_end_date IS NOT NULL 
;

-- 2) Get all records which are going to expire - Currently Active, will be expired;
-- records that are in both Load and TGT; currently Active in TGT & Updated in Load
-- Will need to update the end dates of records fetched from TGT into Stage	 
-- >> TGT to Stage

--ALTER TABLE sales_mart.dim_customer
--ALTER COLUMN hash_key TYPE uuid USING hash_key::uuid;

WITH 
end_date_marked -- GET effective_end_date
AS
(   -- Needed TO SUPPORT Backfill
	-- We are dealing with only active records here; thus will have only
	-- one record for every unique customer id, Because there can only be
	-- one active record for any customer at any given time
	SELECT
		tgt.customer_id, min(ld.created_time) AS effective_end_date
	FROM 
		( 
		 -- SELECT ONLY the needed records FROM target TABLE;
		 -- records that ARE currently active AND updated
			SELECT  
				customer_id, effective_start_date, is_active
			FROM sales_mart.dim_customer AS tgt
			WHERE 
				tgt.is_active = 1 -- currently active
				AND EXISTS ( -- Same PK WITH different value; updated 
					SELECT 1 FROM load_schema.customer_load AS ld
					WHERE tgt.customer_id = ld.customer_id 
						AND tgt.hash_key <> ld.hash_key_dim_customer
				)
		) AS tgt
	JOIN load_schema.customer_load AS ld
		ON tgt.customer_id = ld.customer_id 
	WHERE ld.created_time > tgt.effective_start_date  -- ALL records which were created in Load after the current record  
	GROUP BY tgt.customer_id -- Consider ALL records OF same customer_id 
)
INSERT INTO sales_mart_stage.dim_customer_stage 
SELECT
	tgt.customer_id
	, tgt.account_status 
	, tgt.customer_type 
	, tgt."name" 
	, tgt.gender 
	, tgt.email 
	, tgt.dob 
	, tgt.phone_number 
	, tgt.city 
	, tgt.state 
	, tgt.country 
	, tgt.zipcode
	, tgt.update_time
	, tgt.effective_start_date 
	, em.effective_end_date --> daily granularity; will be 1 day - 
	, 0 AS is_active
	, tgt.hash_key
FROM 
	sales_mart.dim_customer AS tgt
JOIN 
	end_date_marked AS em
	ON 
		tgt.customer_id = em.customer_id 
WHERE 
	tgt.is_active = 1 -- currently active
	AND EXISTS 
		( -- Same PK WITH different value; updated 
			SELECT 1 FROM load_schema.customer_load AS ld
			WHERE tgt.customer_id = ld.customer_id 
				AND (
					tgt.hash_key <> ld.hash_key_dim_customer
				) 
	)
;
 
-- 3) Copy Active records from TGT to Stage;
-- records which are active and not updated. 
-- TGT is going to be Truncate & Load, thus copying all active reords to Stage
-- Here we will copy to Stage 2 sets of records:
--		a) Only in Target, not updated
--		b) In both load & target, not updated. This is mostly for safety reasons
--		   Any incremental CDC should not have records that are not updated.
-- >> TGT to Stage

INSERT INTO sales_mart_stage.dim_customer_stage
SELECT
	* -- All COLUMNS AS IS
FROM 
	sales_mart.dim_customer AS tgt
WHERE 
	tgt.is_active = 1
	AND NOT EXISTS ( -- ONLY IN Target, not updated 
		SELECT 1 FROM load_schema.customer_load AS ld 
		WHERE tgt.customer_id =  ld.customer_id 
	)
UNION ALL 
SELECT
	* -- All COLUMNS AS IS
FROM 
	sales_mart.dim_customer AS tgt
WHERE 
	tgt.is_active = 1
	AND EXISTS( -- IN BOTH LOAD AND target
		SELECT 1 FROM load_schema.customer_load AS ld
		WHERE tgt.customer_id = ld.customer_id 
		AND tgt.hash_key = ld.hash_key_dim_customer 
	) 
;

-- 4) copy only updated records from Load Table 
-- 	  copy into Stage from Load, records that are updated in this load cycle
--    compare New records from Load with TGT & copy the records which are updated into Stage
--    Set this records to be active
--    >> Load to Stage


INSERT INTO sales_mart_stage.dim_customer_stage
SELECT
	ld.customer_id 
	, ld.account_status 
	, ld.customer_type 
	, ld."name" 
	, ld.gender 
	, ld.email 
	, ld.dob 
	, ld.phone_number 
	, split_part(ld.address, '|', 2) AS city
	, split_part(ld.address, '|', 3) AS state
	, split_part(ld.address, '|',4) AS country
	, split_part(ld.address, '|', 5) AS zipcode
	, now() AS update_time 
	, ld.created_time AS effective_start_date  
	, CASE -- IF max created date WITHIN GROUP BY customer_id THEN NULL, ELSE min(other cretaed_time > tgis cretaed_time)
    	dense_rank() OVER(PARTITION BY ld.customer_id ORDER BY ld.created_time DESC)
        WHEN 1 THEN NULL
        ELSE lag(ld.created_time, 1) OVER(PARTITION BY ld.customer_id ORDER BY ld.created_time DESC)
      END AS effective_end_date -- effective_end_date IS NULL; shoul de supported BY backfill
	, CASE 
    	dense_rank() OVER(PARTITION BY ld.customer_id ORDER BY ld.created_time DESC)
        WHEN 1 THEN 1
        ELSE 0
      END AS is_active-- SET flag AS 1, BY USING DATA backfill logic
    , ld.hash_key_dim_customer AS hash_key
FROM 
	load_schema.customer_load AS ld
WHERE EXISTS (
	SELECT 1 FROM sales_mart_stage.dim_customer_stage AS stg_1
	WHERE ld.customer_id = stg_1.customer_id 
	AND stg_1.is_active = 0
	AND NOT EXISTS (
		SELECT 1 FROM sales_mart_stage.dim_customer_stage AS stg_2
		WHERE stg_1.customer_id = stg_2.customer_id 
		AND stg_2.is_active = 1 
	)
)
;


-- 5) Copy fresh reords from Load to Stage;
--    New inserts, Not updates
--    records with PK in load, not in TGT
--    >> Load to Stage
INSERT INTO sales_mart_stage.dim_customer_stage
SELECT
	ld.customer_id 
	, ld.account_status
	, ld.customer_type 
	, ld."name" 
	, ld.gender 
	, ld.email 
	, ld.dob 
	, ld .phone_number 
	, split_part(ld.address, '|', 2) AS city
	, split_part(ld.address, '|', 3) AS state
	, split_part(ld.address, '|',4) AS country
	, split_part(ld.address, '|', 5) AS zipcode
	, now() AS update_time 
	, ld.created_time AS effective_start_date
	, NULL AS effective_end_date 
	, 1 AS is_active -- SET ALL is_active = 1
	, ld.hash_key_dim_customer AS hash_key
FROM load_schema.customer_load AS ld
WHERE NOT EXISTS (
	SELECT 1 FROM sales_mart.dim_customer AS tgt   --load_schema.customer_load AS ld 
	WHERE ld.customer_id = tgt.customer_id 
)
;

-- 6) Truncate and load TGT table from Stage 
--	>> Stage to TGT 
TRUNCATE TABLE sales_mart.dim_customer ;
INSERT INTO sales_mart.dim_customer 
SELECT * FROM sales_mart_stage.dim_customer_stage ;


-- xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

-- #########  Dim Customer Profile Table #############


-- #########  Dim Product Table #############