
/*

Object: stored procedure 

AWS Redshift version: 
PostgreSQL 8.0.2 on i686-pc-linux-gnu, compiled by GCC gcc (GCC) 3.4.2 20041017 (Red Hat 3.4.2-6.fc3), Redshift 1.0.48805

Author: Andrey Maykov

Date: 2023-03-25

Description:
 
The procedure 
1. loads data from the crime_incidents.csv file
located in an S3 bucket into the Redshift table
wxcr_be.ci_stage;
2. removes
	a. 	columns that provide information contained
		in agencies.csv and 
	b. 	duplicate rows 
from ci_stage and loads the cleansed data 
into the wxcr_be.ci1 temp table;
3. adds records to the wxcr_be.log_table to document 
the cleansing done.


The name of the bucket and the IAM Role name required to copy data
from S3 to Redshift are passed through the procedure parameters. 
 
*/

CREATE OR REPLACE PROCEDURE wxcr_be.load_clean_crime_incidents(
	  s3_bucket IN VARCHAR(256)
	, iam_role IN VARCHAR(256)
)
AS $$

DECLARE sql VARCHAR(MAX);

-- The number of crime_incident.csv records
-- that have duplicates 
DECLARE dupe_count INT;

BEGIN

 
	SET search_path = wxcr_be;

-- The table to load the crime_incidents.csv data into
	CREATE TABLE ci_stage (
		   incident_id_orig BIGINT
		,  data_year  INT 						
		,  ori  VARCHAR(9)  DISTKEY
		,  pub_agency_name  VARCHAR(100)
		,  pub_agency_unit  VARCHAR(100)
		,  agency_type_name  VARCHAR(50)
		,  state_abbr  VARCHAR(2)
		,  state_name  VARCHAR(50)
		,  division_name VARCHAR(100)
		,  region_name VARCHAR(50)
		,  population_group_code VARCHAR(200)
		,  population_group_desc  VARCHAR(150)
		,  incident_date  TIMESTAMP
		,  adult_victim_count  INT
		,  juvenile_victim_count  INT 
		,  total_offender_count  INT 
		,  adult_offender_count  INT 
		,  juvenile_offender_count  INT 
		,  offender_race  VARCHAR(200)
		,  offender_ethnicity  VARCHAR(500)
		,  victim_count  INT 
		,  offenses  VARCHAR(800)
		,  total_individual_victims  INT
		,  location_name VARCHAR(500)
		,  bias_desc  VARCHAR(800)
		,  victim_types VARCHAR(500)
		,  multiple_offense VARCHAR(20)
		,  multiple_bias  VARCHAR(20)
	);
	
EXECUTE
	'COPY ci_stage
	FROM \'s3://'||s3_bucket||'/crime_incidents.csv\'
	IAM_ROLE \''||iam_role||'\'
	IGNOREHEADER 1
	FORMAT AS CSV
	TIMEFORMAT \'auto\'
	';

-- Discard the ci_stage columns containing
-- information that is also available from 
-- agencies.csv.
-- Check whether ci_stage contains duplicate rows
-- and remove them (if there are any)
	CREATE TEMP TABLE ci1 AS
	SELECT	
		   COUNT(*) - 1 group_dupe_cnt
		,  ori
		,  population_group_code
		,  population_group_desc
		,  incident_date
		,  adult_victim_count
		,  juvenile_victim_count
		,  total_offender_count 
		,  adult_offender_count
		,  juvenile_offender_count 
		,  offender_race
		,  offender_ethnicity
		,  victim_count
		,  offenses
		,  total_individual_victims
		,  location_name
		,  bias_desc
		,  victim_types
		,  multiple_offense
		,  multiple_bias
	FROM ci_stage
	GROUP BY						
		   ori
		,  population_group_code
		,  population_group_desc
		,  incident_date
		,  adult_victim_count
		,  juvenile_victim_count
		,  total_offender_count 
		,  adult_offender_count
		,  juvenile_offender_count 
		,  offender_race
		,  offender_ethnicity
		,  victim_count
		,  offenses
		,  total_individual_victims
		,  location_name
		,  bias_desc
		,  victim_types
		,  multiple_offense
		,  multiple_bias
	;

sql =
	'SELECT 
		COUNT(group_dupe_cnt) FROM ci1
		WHERE group_dupe_cnt > 0';
EXECUTE sql||' ;' INTO dupe_count;

IF dupe_count > 0 THEN
	EXECUTE 
			'INSERT INTO log_table
			SELECT 
				  \'wxcr_be\'
				, \'crime_incidents\'
				, \''||dupe_count||' records in crime_incidents.csv
				  had duplicates.\'
				, \''||SYSDATE||'\' 
				;';
ELSE
	EXECUTE
		'INSERT INTO log_table
		SELECT 
			  \'wxcr_be\'
			, \'crime_incidents\'
			, \'No duplicate records found in weather_stations.csv.\'
			, \''||SYSDATE||'\' 
			;';
END IF;

-- Change the formats of ci1 data from string to 
-- appropriate ones and move the data into a new table ci2
	CREATE TABLE ci2 (
		   id INT IDENTITY(1, 1)						
		,  ori  VARCHAR(9)
		,  population_group_code VARCHAR(200)
		,  population_group_desc  VARCHAR(150)
		,  incident_date  DATE
		,  incident_time TIME
		,  adult_victim_count  INT
		,  juvenile_victim_count  INT 
		,  total_offender_count  INT 
		,  adult_offender_count  INT 
		,  juvenile_offender_count  INT 
		,  offender_race  VARCHAR(200)
		,  offender_ethnicity  VARCHAR(500)
		,  victim_count  INT 
		,  offenses  VARCHAR(800)
		,  total_individual_victims  INT
		,  location_name VARCHAR(500)
		,  bias_desc  VARCHAR(800)
		,  victim_types VARCHAR(500)
		,  multiple_offense VARCHAR(20)
		,  multiple_bias  VARCHAR(20)
	);
	

	INSERT INTO ci2 (
		   ori
		,  population_group_code
		,  population_group_desc
		,  incident_date
		,  incident_time
		,  adult_victim_count
		,  juvenile_victim_count
		,  total_offender_count 
		,  adult_offender_count
		,  juvenile_offender_count 
		,  offender_race
		,  offender_ethnicity
		,  victim_count
		,  offenses
		,  total_individual_victims
		,  location_name
		,  bias_desc
		,  victim_types
		,  multiple_offense
		,  multiple_bias
	)
	SELECT 
		   ori
		,  population_group_code
		,  population_group_desc
		,  CAST(ci1.incident_date AS DATE)
		,  NULL
		,  adult_victim_count
		,  juvenile_victim_count
		,  total_offender_count 
		,  adult_offender_count
		,  juvenile_offender_count 
		,  offender_race
		,  offender_ethnicity
		,  victim_count
		,  offenses
		,  total_individual_victims
		,  location_name
		,  bias_desc
		,  victim_types
		,  multiple_offense
		,  multiple_bias
	FROM ci1;

END;
$$ LANGUAGE plpgsql;	




			

