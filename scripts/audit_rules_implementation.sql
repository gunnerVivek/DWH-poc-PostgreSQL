
-- ########################################################################
-- 		This script has all the Audit rules for all of the load Tables
-- ########################################################################
-- fill up the Audit log table
	-- with cte
	--	select all rows with where clause
	-- from cte select and insert to audit log table

-- then insert to Audit table the records that failed audit rules
	-- insertion has to happen together with Audit logs for each table because otherwise we will need to
	--	GET table name dynamically and it is not possible in postgres   
	-- for each primary key form 
	
-- then push to Stage - part of SCD


-- Log Table :
	--	record_id varchar(100)
	--	, table_name varchar(50)
	--	, audit_rule_id varchar(50)
	--	, description text
 
-- ########### 


DROP PROCEDURE IF EXISTS sp_audit_load_checks;

CREATE OR REPLACE PROCEDURE sp_audit_load_checks(
	IN rule_table TEXT --varchar(50) -- audit rules
	, IN load_table TEXT --varchar(50) -- LOAD table
	, IN audit_logs_table TEXT --varchar(50) -- Log TABLE for Audit violation
	, IN audit_table TEXT --varchar(50) -- CORRESPONDING Audit TABLE FOR the LOAD tables
	, IN rule_table_id TEXT --varchar(50) -- PK of audit rules TABLE
	, IN load_table_id TEXT --varchar(50) -- PK of LOAD TABLE
	, IN audit_table_id TEXT -- COLUMN name OF PK of Audit TABLE
	)
LANGUAGE plpgsql
AS 
$BODY$
DECLARE
	get_rule_ids_sql TEXT; -- SQL string TO FETCH ALL the RULE Ids
	create_audit_logs_sql TEXT; -- SQL TO PREPARE log staement FOR EACH RULE id IN Audit Rules
	insert_audit_records_sql TEXT; -- SQL that will INSERT records into Audit table that failed Audit
	record_temp RECORD; -- will be used IN LOOP TO HOLD a record
	rule_pattern TEXT;
	load_table_simple_name TEXT; -- extracted one part name if two part name provided or the provided name itself  
BEGIN
	
	
	load_table_simple_name :=
		CASE 
			WHEN split_part(load_table, '.', 2) IS NULL 
				THEN load_table
			WHEN split_part(load_table, '.', 2) = '' --blank text 
				THEN load_table 
			ELSE  split_part(load_table, '.', 2)
		END ;
	
	-- decide on rule pattern
	rule_pattern := 
		CASE load_table_simple_name
			WHEN 'customer_load' THEN 'check_cust_%'
			WHEN 'product_load' THEN 'check_prod_%'
			WHEN 'sales_load' THEN 'check_sales_%'
			ELSE NULL
		END
	;
	
	
	-- GET ALL rules & SQL of Audit rules FOR this LOAD table
	-- this is a very small table
	get_rule_ids_sql := 
		$$ CREATE TEMP TABLE tmp_rules_tbl ON COMMIT DROP AS $$ || 
		$$ SELECT audit_rule_id, sql_constraint, description $$ ||
		$$ FROM $$ || rule_table || $$ AS ar $$ ||
		$$ WHERE ar.$$ || rule_table_id || $$ ILIKE '$$||rule_pattern||$$'$$
					;--'cust_%'
	
	
	--RAISE NOTICE 'Get rule ids: %', get_rule_ids_sql;
	EXECUTE get_rule_ids_sql
	;
	
	-- record_temp.audit_rule_id, record_temp.description, record_temp.sql_constraint;
	FOR record_temp IN SELECT * FROM tmp_rules_tbl -- GET Audit 
	 -- for every rule populate the log table
	 -- then populate the Audit table
	 -- Steps:
	 	-- create a temp table without on commit delete to hold log data
	  	-- from log get ids to update Load audit table, then update Load audit 
	  	-- table update ... on conflict do nothing
	  			-- Log table will have multiple entries for one PK value from load table
	  			-- We want distinct load table ids from log table (tmp_log_tbl)
	  	-- delete temp table of log data
	  LOOP
		  
		 create_audit_logs_sql := 
		 	$$CREATE TEMP TABLE tmp_log_tbl AS 
		 	 SELECT $$ || 
				load_table_id || $$ AS "record_id", $$ ||
				quote_literal(load_table) || $$ AS "table_name", $$ ||
				quote_literal(record_temp.audit_rule_id) || $$ AS "audit_rule_id", $$ ||
				quote_literal(record_temp.description) || $$ AS "description" $$ || 
			$$ FROM $$ || load_table || $$ AS cl
			   WHERE $$ ||
			  	record_temp.sql_constraint -- the WHERE clause IS DEFINED IN the rules table sql_constraint column
			;
		  					
		 --RAISE NOTICE 'audit log SQL: %', create_audit_logs_sql ;
		
		 -- ##### 1. Get the Audit Logs and store temporarily 	
		 -- RAISE NOTICE 'audit log SQL: %', create_audit_logs_sql;
		 EXECUTE 
		 create_audit_logs_sql  
		 ;
		
		-- ######## 2. Insert the Audit failed records into Audit Table
		 insert_audit_records_sql := 
		 	-- Need to get distinct load table ids from tmp_log_tbl and join with 
		 	-- Load table to get the required Audit failed records
		 	$$ WITH distinct_load_table_ids AS 
		 	   (
		 		 SELECT "record_id" FROM tmp_log_tbl GROUP BY "record_id"
		 	   ) 
		 	$$ ||   
		 	$$ INSERT INTO $$ ||audit_table|| 
		 	$$ SELECT 
		 		lt.*
		 		FROM $$ ||load_table|| $$ AS lt-- LOAD table
		 		JOIN distinct_load_table_ids AS di
		 			ON lt.$$||load_table_id||$$ = di."record_id"
		 		ON CONFLICT ($$||audit_table_id||$$) DO NOTHING 
		 	$$
		 ;
		
		-- RAISE NOTICE 'Insert Audit Query: %', insert_audit_records_sql;
		EXECUTE 
		insert_audit_records_sql
		;
	
		-- ########## 3. Insert data from temporary log table to Audit log table
		--RAISE NOTICE 'Insert data from temp Log to audit table log: %',
		EXECUTE 
		$$ INSERT INTO $$ ||audit_logs_table||
		$$ SELECT * FROM tmp_log_tbl$$
		;	
	
		-- drop the temporary 
		DROP TABLE IF EXISTS tmp_log_tbl;  
	  
	END LOOP;


	COMMIT;	
END
$BODY$
;


-- Example call to Procedure sp_audit_load_checks(text, text, text, text, text, text, text) 
CALL public.sp_audit_load_checks(
	'foreign_schema.audit_rules'
	, 'load_schema.customer_load'
	, 'load_schema.audit_logs'
 	, 'load_schema.customer_audit'
 	, 'audit_rule_id'
 	, 'customer_id'
 	, 'customer_id'
)
; 




-- ##############################################################################
-- Procedure: sp_duplicate_check(_text)                                         #
-- Description:                                                                 #
-- 		This procedure is to check for duplicate records. The duplicate         #
--		check is performed using the subset of columns provided as an           #
--		input parameter.                                                        #
-- 			*Although the Function can accept a subset of parameters, here we   #
--		use all the cplumns to perform duplicate check.							# 	 
-- Parameters:                                                                  #
-- 		IN columns_to_group TEXT[] :- An array of columns to consider for       # 
-- 			duplicate check.                                                    #
--		IN load_table TEXT :- Name of the table to check the duplicates for.    #
--			Name should be fully qualified.										#
-- ##############################################################################
DROP FUNCTION sp_duplicate_check(_text, text)  ;
CREATE FUNCTION sp_duplicate_check(
		IN columns_to_group TEXT[]
		, IN load_table TEXT
	)
RETURNS INT
LANGUAGE plpgsql
AS $BODY$
DECLARE 
	group_by_columns_string TEXT;
	duplicate_query TEXT;
	duplicate_rows INT; -- Number of duplicate rows. This willbe returned 
BEGIN 
	group_by_columns_string := '';
	FOR i IN 1..array_length(columns_to_group, 1)
		LOOP
			group_by_columns_string := group_by_columns_string || columns_to_group[i];
			IF i < array_length(columns_to_group, 1) THEN
				group_by_columns_string := group_by_columns_string || ', ';
			END IF;
		END LOOP;
	
	--RAISE NOTICE 'Group By columns: %', group_by_columns_string;
	
	-- remove duplicates

	-- Alter load table to include a serial number column
	-- Insert into this column the serial number
	-- delete from <load> table all rows with serial number > 1
	-- drop the serial number column
	
	-- add the temporary columns
	--RAISE NOTICE 'Add serial columnn: %',  
	EXECUTE
	$$ ALTER TABLE $$||load_table||
	$$ ADD COLUMN line_no INT GENERATED ALWAYS AS IDENTITY
	, ADD COLUMN serial INT
	$$
	; 

	-- Update serial column
	--RAISE NOTICE 'Populate Serial column: %', 
	EXECUTE
	$$ WITH duplicate_marked AS
	(
		SELECT 
	  		line_no
	  		, ROW_NUMBER() OVER(PARTITION BY $$||group_by_columns_string||$$ ) AS serial_no
	  	FROM $$||load_table|| 
	$$ )
	UPDATE $$||load_table||
	$$ SET serial = duplicate_marked.serial_no
	FROM duplicate_marked 
	WHERE $$||load_table||$$.line_no = duplicate_marked.line_no$$
	;
	
	-- TODO:  complete Number of duplicates removed to be returned. Covert Proc to function
	--RAISE NOTICE 'Num duplicate rows: %',
	EXECUTE
	'( SELECT count(*) FROM '||load_table||' WHERE serial > 1)'
	INTO duplicate_rows
	;
	
	--RAISE NOTICE 'Delete duplicate records: %',  
	EXECUTE
	$$ DELETE FROM $$ ||load_table||
	$$	WHERE serial > 1 $$
	;

	--RAISE NOTICE 'Drop the serial and line_no columns: %',  
	EXECUTE
	$$ ALTER TABLE $$ || load_table ||
	$$ DROP COLUMN serial, DROP COLUMN line_no$$
	;

	RETURN duplicate_rows; 
	COMMIT;
END
$BODY$
;


-- Example Call to FUNCTION sp_duplicate_check(_text, text) 
--select sp_duplicate_check(
--	ARRAY['customer_id','account_status','customer_type','name','gender','email','dob',
--			'phone_number','address','customer_class','customer_band','credit_score',
--			'income_level','customer_occupation_category','created_time'
--		]
--		, 'load_schema.customer_load'
--)
--;

-- ################################################################################################
-- Note: Should be perfromed after general duplicate column Removal
--
-- Procedure: sp_unique_constraint_check()
-- Description:
-- 		This stored procedure is used to find duplicate occurances of a value in 
--		an Unique column. The check takes into account the Primary Key column for 
--		finding values with > 1 occurance. Count of PK is checked for every unique
--		value of the Unique column; this is necessary instead of checking for duplicates
--		based on the Ub=nique column itself because load table CDC can have mre than one 
--		update for the same PK, which will cause faulty results if checked on the Unique COLUMN 
--		only.
--Parameters:
--	unique_cols:
--	primary_key TEXT[] : Primary Key of the table. This can be composite Key. Primary keys ONLY 
--						 functionality is for determining duplicate Unique column values 
-- ################################################################################################
--DROP PROCEDURE IF EXISTS public.sp_unique_constraint_check;
CREATE OR REPLACE PROCEDURE sp_unique_constraint_check(
	unique_col TEXT
	, primary_key TEXT[]
	, table_name TEXT
	, audit_log_table TEXT
	, audit_rule_table TEXT
	, audit_rule_id TEXT
	, audit_table TEXT
	, audit_table_id TEXT
	, is_error_check boolean DEFAULT FALSE --  TRUE --> error OR False --> warning; 
										   -- DELETE records IN LOAD TABLE along with logs IF error
										   --, otherwise just log 
	)
LANGUAGE plpgsql
AS 
$BODY$
DECLARE
	group_by_cols TEXT; -- This IS the list OF COLUMNS that will be used to group by the table to produce
						-- Unique combination of Unique key and Primary key. The Unique should always be
						-- the first
	pk_flattened TEXT; -- PK flattened into one comma seperated string; accomodates composite PK 
	unique_val_count_query TEXT; -- This query counts occurance of a Unique Value with 'n' PKs   
	populate_duplicate_records_temp_sql TEXT; -- records WITH duplicate UNIQUE KEY 
	insert_to_audit_log_sql TEXT;
	insert_to_audit_table_sql TEXT;
BEGIN 
	-- 1. get unique combination of Unique key and primary key(s) as CTE
	-- 2. Get Unique Keys that have count() > 1
	-- 3. For all these keys in the Load table --> records to Log and Audit Table
	
	pk_flattened := '';
	
	FOR i IN 1..array_length(primary_key, 1)
		LOOP
			pk_flattened := pk_flattened||primary_key[i];
			IF i < array_length(primary_key, 1) THEN
				pk_flattened := pk_flattened||', ' ;
			END IF;
		END LOOP;
	
	-- make the group by clause String
	group_by_cols := unique_col||', '||pk_flattened ;
	
	-- RAISE NOTICE 'group by: %', group_by_cols;
	
	-- Hold the offending Unique Keys in temp table
	populate_duplicate_records_temp_sql := $$
	CREATE TEMP TABLE unique_tmp ON COMMIT DROP 
	AS 
	WITH uniqueKey_pk_unique_combination AS 
	(
		SELECT $$||group_by_cols||$$ FROM $$||table_name||$$ GROUP BY $$||group_by_cols|| 
	$$)
	SELECT $$||unique_col||$$ FROM uniqueKey_pk_unique_combination
	GROUP BY $$||unique_col||$$ HAVING count(*) > 1$$ 
	;

	--RAISE NOTICE 'Duplicate Record populate: %', 
	EXECUTE 
	populate_duplicate_records_temp_sql ;
	
	
	-- Log to the Audit_log table
	-- Description will come from rules table
	-- for log table we will need primary keys comma separated	
	insert_to_audit_log_sql :=
	' INSERT INTO '||audit_log_table||
	' WITH unique_combi AS
	(
	  SELECT ut.'||unique_col||', '||pk_flattened||
	  ' FROM unique_tmp AS ut
	  JOIN  '||table_name||
	  	' ON ut.'||unique_col||' = '||table_name||'.'||unique_col||
	  ' group by ut.'||unique_col||', '||pk_flattened||
	' )
	SELECT 
		concat_ws('||quote_literal(', ')||', '||pk_flattened||')  AS "record_id", '
		||quote_literal(table_name)||' AS "table_name", '
		||quote_literal(audit_rule_id)||' AS "audit_rule_id"
		, ar.description 
	FROM unique_combi AS uc
	JOIN '||audit_rule_table||' AS ar 
		ON ar.audit_rule_id = '||quote_literal(audit_rule_id)
	;

	--RAISE NOTICE 'Insert to Audit Log: %',
	EXECUTE
	insert_to_audit_log_sql ;


	-- Write to Audit table
	-- Get data from Load Table that have multiple PK for Unique key
	--	 inner join Load Table and tmp table on unique key
	insert_to_audit_table_sql := $$
	INSERT INTO $$||audit_table||
	$$ SELECT 
		tb.* 
	FROM $$||table_name||$$ AS tb
	JOIN unique_tmp AS ut
		ON tb.$$||unique_col||$$ = ut.$$||unique_col||
	$$ ON CONFLICT ($$||audit_table_id||$$) DO NOTHING $$
	;
		
	--RAISE NOTICE 'Insert to Audit: %', 	insert_to_audit_table_sql ; 
	EXECUTE
	insert_to_audit_table_sql ;

	-- Delete records from load table if this is an error check
	IF is_error_check IS TRUE THEN
		-- delete thge records with duplicate unique keys
		-- duplicate key are stored in above temp table unique_tmp
		EXECUTE 
		'DELETE t1 
		FROM '||table_name||' AS t1
		WHERE EXISTS  (SELECT 1 FROM unique_tmp WHERE t1.'||unique_col||' = unique_tmp.'||unique_col||')'
		;
	END IF;

	COMMIT;	
END; 
$BODY$
;


-- Example CALL to sp_unique_constraint_check() 
--CALL sp_unique_constraint_check('email', ARRAY['customer_id'], 'load_schema.customer_load'
--	, 'load_schema.audit_logs', 'foreign_schema.audit_rules', 'unique_cust_01', 'load_schema.customer_audit'
--	, 'customer_id'
--);
