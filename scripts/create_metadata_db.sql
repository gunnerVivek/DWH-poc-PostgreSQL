
CREATE DATABASE execution_metadata;

GRANT ALL PRIVILEGES ON DATABASE execution_metadata TO vivek; 

use DATABASE execution_metadata;

CREATE TABLE IF NOT EXISTS audit_rules(
	audit_rule_id varchar(50)
	, audit_rule_name varchar(50)
	, description TEXT
	, CONSTRAINT audit_rule_pk PRIMARY KEY (audit_rule_id)
);




-- ###### Create Extension to access Meta data DB from DWH DB
CREATE EXTENSION postgres_fdw;
--  select * from pg_extension;

create server execution_info_server foreign data wrapper postgres_fdw 
	options (host '127.0.0.1', port '5432', dbname 'execution_metadata');
-- select * from pg_foreign_server;

create user mapping for vivek server execution_info_server options (user 'vivek', password 'postgres');
-- select * from pg_user_mappings;

grant usage on foreign server execution_info_server to vivek;


create schema if not exists foreign_schema;

import foreign schema public limit to (audit_rules) from server execution_info_server into foreign_schema;



-- to refresh the table after schema change
DROP FOREIGN TABLE IF EXISTS foreign_schema.audit_rules;

import foreign schema public limit to (audit_rules) from server execution_info_server into foreign_schema;
