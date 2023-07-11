
/*

Object: stored procedure 

AWS Redshift version: 
PostgreSQL 8.0.2 on i686-pc-linux-gnu, compiled by GCC gcc (GCC) 3.4.2 20041017 (Red Hat 3.4.2-6.fc3), Redshift 1.0.48805

Author: Andrey Maykov

Date: 2023-03-22

Description:
 
The procedure 
1. loads data from the weather_stations.csv file
located in an S3 bucket into the Redshift table
wxcr_be.ws_stage;
2. removes the duplicate rows and the rows with inconsistent
data from ws_stage and loads the cleansed data 
into the WXCR.dim_weather_stations table.
3. adds records to the wxcr_be.log_table to document 
the cleansing done.


The name of the bucket and the IAM Role name required to copy data
from S3 to Redshift are passed through the procedure parameters. 
 
*/

CREATE OR REPLACE PROCEDURE wxcr_be.load_clean_weather_stations(
	  s3_bucket IN VARCHAR(256)
	, iam_role IN VARCHAR(256)
)
AS $$

DECLARE sql VARCHAR(MAX);

-- The number of pairs (latitude, longitude)
-- for which weather_stations.csv provides multiple sets of details 
-- (i.e. identical or different sets for the same station)  
DECLARE latlong_dupe_count INT;

-- The number of pairs (latitude, longitude) 
-- for which weather_statioins.csv provides duplicate sets of details 
-- (i.e. multiple identical sets for the same station) 
DECLARE full_dupe_count INT;

-- The number of pairs (latitude, longitude)
-- for which weather_statioins.csv provides inconsistent sets of details 
-- (i.e. several different sets for the same weather station)
DECLARE latlongs_ir_count INT;

-- The name of the temp table that holds 
-- all the weather station detail sets with no duplicates 
-- (either due to removing the duplicates or because
-- there weren't any in weather_station.csv initially)
DECLARE ws1_name VARCHAR(32);

-- The name of the temp table that holds 
-- all the weather_station detail sets with no duplicates or    
-- inconsistent detail sets (either due to 
-- removing ones or because there weren't any 
-- in weather_stations.csv initially).
DECLARE ws2_name VARCHAR(32);

BEGIN

 
	SET search_path = wxcr_be;

-- The table to load the weather_stations.csv data into

	CREATE TABLE ws_stage (
		   stn_city VARCHAR(100)
		 , stn_country VARCHAR(100)
		 , stn_lat FLOAT
		 , stn_long FLOAT
	)
	;

EXECUTE
	'COPY ws_stage
	FROM \'s3://'||s3_bucket||'/meteodata/weather_stations.csv\'
	IAM_ROLE \''||iam_role||'\'
	IGNOREHEADER 1
	FORMAT AS CSV
	';

-- Check whether ws_stage contains duplicate pairs (stn_lat, stn_long)
-- (which can indicate duplicates or inconsistent detail sets)
sql =
	'WITH latlong_dupes AS (
		SELECT DISTINCT stn_lat
		FROM ws_stage
		GROUP BY
			  stn_lat
			, stn_long
		HAVING COUNT(*) > 1
	)
	SELECT 
		COUNT(*) FROM latlong_dupes';
EXECUTE sql||' ;' INTO latlong_dupe_count;


-- If the ws_stage contains duplicate pairs (stn_lat, stn_long), start cleaning; 
-- if not, just set ws2_name = 'ws_stage'
IF latlong_dupe_count > 0 THEN
	
	 -- collect the (stn_lat, stn_long) pairs corresponding  
	 -- to the duplicate station detail sets (if there are any)
		CREATE TABLE full_dupe_latlongs AS
		SELECT * FROM ws_stage
		GROUP BY
			   stn_city
			 , stn_country
			 , stn_lat
			 , stn_long
		HAVING COUNT(*) > 1
	;
	sql =
		'SELECT COUNT(*)
		FROM full_dupe_latlongs';
	EXECUTE sql||' ;' INTO full_dupe_count;

	-- If there are any duplicate agency detail sets, remove them;
	-- if not, just set ws1_name = 'ws_stage'
	IF full_dupe_count > 0 THEN
		EXECUTE 
			'INSERT INTO log_table
			SELECT 
				  \'wxcr_be\'
				, \'weather_stations\'
				, \'Duplicate records found for '||full_dupe_count||' weather stations.
				  	For the corresponnding station latitudes and longituedes, see the full_dupe_latlongs table.\'
				, \''||SYSDATE||'\' 
				;';
		
			CREATE TEMP TABLE ws1
			(LIKE ws_stage);
			
			INSERT INTO ws1 (SELECT DISTINCT * FROM ws_stage);
			ws1_name = 'ws1';
	ELSE
		
			ws1_name = 'ws_stage';
		
			DROP TABLE full_dupe_latlongs;
	END IF;

		
	-- Create a temp table that contains the (stn_lat, stn_long) pairs
	-- corresponding to inconsistent agency detail sets
	-- (if there are any).
	EXECUTE
		'CREATE TEMP TABLE latlongs_ir AS 
		SELECT stn_lat, stn_long FROM '||ws1_name||' 
		GROUP BY
			  stn_lat
			, stn_long
		HAVING COUNT(*) > 1
		;'
		;
	
	sql =
		'SELECT COUNT(*)
		FROM latlongs_ir';
	EXECUTE sql||' ;' INTO latlongs_ir_count;
		
	-- If there are any inconsistent weather station detail sets,
	-- collect them in the ws_ir table
	-- remove them from the table with name ws1_name
	-- (i.e. ws1 or ws_stage, whichever applicable)
	-- and load the result into the ws2 table;
	-- if not, just set ws2_name = ws1_name.
	IF 	latlongs_ir_count > 0 THEN
		EXECUTE
			'CREATE TABLE ws_ir 
			SORTKEY (stn_lat, stn_long)
			AS SELECT * FROM '||ws1_name||' wss
			WHERE EXISTS (
				SELECT 1 FROM latlongs_ir 
				WHERE 
					latlongs_ir.stn_lat = wss.stn_lat AND
					latlongs_ir.stn_long = wss.stn_long
			);'
			;
		EXECUTE 
			'INSERT INTO log_table
			SELECT 
				  \'wxcr_be\'
				, \'weather_stations\'
				, \''||latlongs_ir_count||' inconsistent records found for agencies and moved to the agencies_ir table.\'
				, \''||SYSDATE||'\' 
				;';
		
			DROP TABLE IF EXISTS ws2;
		EXECUTE	
			'CREATE TEMP TABLE ws2 AS (
				SELECT wss.* FROM '||ws1_name||' wss 
				WHERE NOT EXISTS(
					SELECT 1 FROM latlongs_ir
					WHERE 
						latlongs_ir.stn_lat = wss.stn_lat AND
						latlongs_ir.stn_long = wss.stn_long
				)
			);'
			;
		ws2_name = 'ws2';
	ELSE
		ws2_name = ws1_name;
	END IF;
ELSE 
	EXECUTE
	'INSERT INTO log_table
	SELECT 
		  \'wxcr_be\'
		, \'weather_stations\'
		, \'No duplicate or inconsistent records found in weather_stations.csv.\'
		, \''||SYSDATE||'\' 
		;';
	ws2_name = 'ws_stage';
END IF;


CREATE TABLE wxcr.dim_weather_stations (
		  stn_name VARCHAR(100) PRIMARY KEY
		, stn_city VARCHAR(100)
		, stn_country VARCHAR(100)
		, stn_lat FLOAT
		, stn_long FLOAT
	)
	DISTSTYLE ALL
	SORTKEY (stn_country, stn_city)
;

EXECUTE
	'INSERT INTO wxcr.dim_weather_stations (
		  stn_name
		, stn_city
		, stn_country
		, stn_lat
		, stn_long
	)
	SELECT REPLACE(stn_city, \' \', \'_\'), * FROM '||ws2_name||';'
;


 
	SET search_path = WXCR;
END;
$$ LANGUAGE plpgsql;	




			

