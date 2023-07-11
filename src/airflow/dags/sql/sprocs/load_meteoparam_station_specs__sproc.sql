
/*

Object: stored procedure 

AWS Redshift version: 
PostgreSQL 8.0.2 on i686-pc-linux-gnu, compiled by GCC gcc (GCC) 3.4.2 20041017 (Red Hat 3.4.2-6.fc3), Redshift 1.0.48805

Author: Andrey Maykov

Date: 2023-03-25

Description:
 
The procedure 
1. create Redshift tables for
	a. format specifications for the meteoparameters
	(temperature, pressure, humidity, wind_speed, wind_direction,
	description) from the meteoparam_names_fmts.csv;
	b. the names of the weather stations for which the 
	meteoparameter values are provided in the original dataset;
2. populates the tables with the data from the project's S3 bucket.
 
 
*/

CREATE OR REPLACE PROCEDURE wxcr_be.load_meteoparam_station_specs(
	  s3_bucket IN VARCHAR(256)
	, iam_role IN VARCHAR(256)
)
AS $$

BEGIN

SET search_path = wxcr_be;

-- Table to load the weather_tbl_names_fmts.csv data into
CREATE TABLE meteoparam_names_fmts (
	  tbl_name VARCHAR(64)
	, col_fmt VARCHAR(64)
);

EXECUTE
	'COPY wxcr_be.meteoparam_names_fmts
	FROM \'s3://'||s3_bucket||'/auxfs/meteoparam_names_fmts.csv\'
	IAM_ROLE \''||iam_role||'\'
	IGNOREHEADER 1
	FORMAT AS CSV'
;

-- Table to load the station_names.csv data into
CREATE TABLE station_names (
	  id INT IDENTITY(1, 1) PRIMARY KEY
	, station_name VARCHAR(64)
);

EXECUTE
	'COPY station_names
	FROM \'s3://'||s3_bucket||'/auxfs/station_names.csv\'
	IAM_ROLE \''||iam_role||'\'
	IGNOREHEADER 0
	FORMAT AS CSV'
;

END;
$$ LANGUAGE plpgsql;	



			

