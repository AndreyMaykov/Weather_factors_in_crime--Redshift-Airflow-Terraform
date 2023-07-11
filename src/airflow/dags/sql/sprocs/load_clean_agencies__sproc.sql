
/*

Object: stored procedure 

AWS Redshift version: 
PostgreSQL 8.0.2 on i686-pc-linux-gnu, compiled by GCC gcc (GCC) 3.4.2 20041017 (Red Hat 3.4.2-6.fc3), Redshift 1.0.48805

Author: Andrey Maykov

Date: 2023-03-22

Description:
 
The procedure 
1. loads data from the agencies.csv file, which is
located in an S3 bucket, into the Redshift table
wxcr_be.agencies_stage;
2. removes the duplicate rows and the rows with inconsistent
data from agencies_stage and loads the cleansed data 
into the WXCR.dim_agencies table;
3. adds records to the wxcr_be.log_table to document 
the cleansing done.


The name of the bucket and the IAM Role name required to copy data
from S3 to Redshift are passed through the procedure parameters. 
 
*/

					
					

CREATE OR REPLACE PROCEDURE wxcr_be.load_clean_agencies(
	  s3_bucket IN VARCHAR(256)
	, iam_role IN VARCHAR(256)
)
AS $$

DECLARE sql VARCHAR(MAX);

-- The number of Originating Agency Identifiers (ORIs)
-- for which agencies.csv provides multiple sets of details 
-- (i.e. several identical or different sets for the same agency)  
DECLARE ori_dupe_count INT;

-- The number of ORIs 
-- for which agencies.csv provides duplicate sets of details 
-- (i.e. several identical sets for the same agency) 
DECLARE full_dupe_count INT;

-- The number of ORIs 
-- for which agencies.csv provides inconsistent sets of details 
-- (i.e. several different sets for the same agency)
DECLARE ori_ir_count INT;

-- The name of the temp table that holds 
-- all the agency detail sets with no duplicates 
-- (either due to removing the duplicates or because
-- there weren't any in agencies.csv initially)
DECLARE agencies1_name VARCHAR(32);

-- The name of the temp table that holds 
-- all the agency detail sets with no duplicates or    
-- inconsistent detail sets (either due to 
-- removing ones or because there weren't any 
-- in agencies.csv initially).
DECLARE agencies2_name VARCHAR(32);

BEGIN

SET search_path = wxcr_be;

-- The table to load the agencies.csv data into
	CREATE TABLE agencies_stage (
		  ori  VARCHAR(9)
		, agency  VARCHAR(100)
		, agency_type  VARCHAR(100)
		, state  VARCHAR(50)
		, state_abbr  VARCHAR(2)
		, division  VARCHAR(150)
		, region   VARCHAR(150)
		, region_desc  VARCHAR(150)
		, county  VARCHAR(150)
		, nibrs  BOOLEAN
		, ori_lat  FLOAT
		, ori_long  FLOAT
		, nibrs_start_date  TIMESTAMP
	)
	DISTSTYLE KEY DISTKEY(state_abbr)
	SORTKEY (ori)
	;

EXECUTE
	'COPY agencies_stage
	FROM \'s3://'||s3_bucket||'/agencies.csv\'
	IAM_ROLE \''||iam_role||'\'
	IGNOREHEADER 1
	FORMAT AS CSV
	TIMEFORMAT \'auto\'
	';

-- Check whether the ori column contains duplicate values
-- (which can indicate duplicates or inconsistent detail sets)
sql =
	'SELECT 
		COUNT(ori) - COUNT(DISTINCT ori)
	FROM agencies_stage';
EXECUTE sql||' ;' INTO ori_dupe_count;

-- If the ori column contains duplicate values, start cleaning; 
-- if not, just set agencies2_name = 'agencies_stage'
IF ori_dupe_count > 0 THEN 
	 -- collect the ori values corresponding  
	 -- to the duplicate agency detail sets (if there are any)
		CREATE TABLE full_dupe_oris AS
		SELECT ori FROM agencies_stage
		GROUP BY
			  ori  
			, agency
			, agency_type
			, state
			, state_abbr
			, division
			, region
			, region_desc
			, county
			, nibrs
			, ori_lat
			, ori_long
			, nibrs_start_date
		HAVING COUNT(*) > 1;
	sql =
		'SELECT COUNT(*)
		FROM full_dupe_oris';
	EXECUTE sql||' ;' INTO full_dupe_count;

	-- If there are any duplicate agency detail sets, remove them;
	-- if not, just set agencies1_name = 'agencies_stage'
	IF full_dupe_count > 0 THEN
		EXECUTE 
			'INSERT INTO log_table
			SELECT 
				  \'wxcr_be\'
				, \'agencies\'
				, \'Duplicate records found for '||full_dupe_count||' agencies.
				  	For the corresponnding ORIs, see the full_dupe_oris table.\'
				, \''||SYSDATE||'\' 
				;';
			CREATE TEMP TABLE agencies1
			(LIKE agencies_stage);	
			INSERT INTO agencies1 (SELECT DISTINCT * FROM agencies_stage);
			agencies1_name = 'agencies1';
	ELSE
		EXECUTE
			agencies1_name = 'agencies_stage';
	END IF;
		
	-- Create a temp table that contains the ori values
	-- corresponding to inconsistent agency detail sets
	-- (if there are any).
	EXECUTE
		'CREATE TEMP TABLE oris_ir AS 
		SELECT ori FROM '||agencies1_name||' 
		GROUP BY ori
		HAVING COUNT(*) > 1
		;'
		;
	
	sql =
		'SELECT COUNT(*)
		FROM oris_ir';
	EXECUTE sql||' ;' INTO ori_ir_count;
		
	-- If there are any inconsistent agency detail sets,
	-- collect them in the agencies_ir table
	-- remove them from the table with name agencies1_name
	-- (i.e. agencies1 or agencies_stage, whichever applicable)
	-- and load the result into the agencies2 table;
	-- if not, just set agencies2_name = agencies1_name.
	IF 	ori_ir_count > 0 THEN
		EXECUTE
			'CREATE TABLE agencies_ir 
			SORTKEY (ori)
			AS SELECT * FROM '||agencies1_name||' ags
			WHERE EXISTS (
				SELECT 1 FROM oris_ir 
				WHERE oris_ir.ori = ags.ori
			);'
			;
		EXECUTE 
			'INSERT INTO log_table
			SELECT 
				  \'wxcr_be\'
				, \'agencies\'
				, \''||ori_ir_count||' inconsistent records found for agencies and moved to the agencies_ir table.\'
				, \''||SYSDATE||'\' 
				;';
		EXECUTE	
			'CREATE TEMP TABLE agencies2 AS (
				SELECT ags.* FROM '||agencies1_name||' ags 
				WHERE NOT EXISTS(
					SELECT 1 FROM oris_ir
					WHERE oris_ir.ori = ags.ori
				)
			);'
			;
		agencies2_name = 'agencies2';
	ELSE
		agencies2_name = agencies1_name;
	END IF;
ELSE 
	EXECUTE
	'INSERT INTO log_table
	SELECT 
		  \'wxcr_be\'
		, \'agencies\'
		, \'No duplicate or inconsistent records found in agencies.csv.\'
		, \''||SYSDATE||'\' 
		;';agencies2_name = 'agencies_stage';
END IF;

-- Load the processed data into the target main schema table
	CREATE TABLE WXCR.dim_agencies (
		  ori  VARCHAR(9) PRIMARY KEY
		, agency  VARCHAR(100)
		, agency_type  VARCHAR(100)
		, state  VARCHAR(50)
		, state_abbr  VARCHAR(2)
		, division  VARCHAR(150)
		, region   VARCHAR(150)
		, region_desc  VARCHAR(150)
		, county  VARCHAR(150)
		, nibrs  BOOLEAN
		, ori_lat  FLOAT
		, ori_long  FLOAT
		, nibrs_start_date  TIMESTAMP
	)
	DISTSTYLE ALL
	SORTKEY (ori_lat, ori_long)
	;

EXECUTE
	'INSERT INTO WXCR.dim_agencies (
		  ori  
		, agency
		, agency_type
		, state
		, state_abbr
		, division
		, region
		, region_desc
		, county
		, nibrs
		, ori_lat
		, ori_long
		, nibrs_start_date
	)
	SELECT * FROM '||agencies2_name||';'
;

	SET search_path = WXCR;
END;
$$ LANGUAGE plpgsql;	


			

