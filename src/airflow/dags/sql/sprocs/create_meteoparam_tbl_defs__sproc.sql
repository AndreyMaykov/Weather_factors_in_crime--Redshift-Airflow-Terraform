
/*

Object: stored procedure 

AWS Redshift version: 
PostgreSQL 8.0.2 on i686-pc-linux-gnu, compiled by GCC gcc (GCC) 3.4.2 20041017 (Red Hat 3.4.2-6.fc3), Redshift 1.0.48805

Author: Andrey Maykov

Date: 2023-03-21

Description:
 
The procedure 
1. for each meteoparameter, generates a string containing 
column names and formats(e.g. datetime DATETIME, 
Philadelphia DECIMAL(5, 2), New_York DECIMAL(5, 2), ... );
the strings will be used to in the CREATE TABLE statements 
for the staging tables temperture, humidity, etc.
2. place the generated strings into 
the wxcr_be.meteoparam_tbl_defs table;

*/


CREATE OR REPLACE PROCEDURE wxcr_be.create_meteoparam_tbl_defs()
AS $$

BEGIN

SET search_path = wxcr_be;

DROP TABLE IF EXISTS param_station_fmt;
CREATE TEMP TABLE param_station_fmt AS
SELECT nf.tbl_name, stn.id id, stn.station_name, nf.col_fmt 
FROM meteoparam_names_fmts nf, station_names stn
;

DROP TABLE IF EXISTS tbl_cols;
CREATE TEMP TABLE tbl_cols AS
SELECT 
	  tbl_name
	, id
	, CONCAT(CONCAT(station_name, ' '), col_fmt) col
FROM param_station_fmt
;

DROP TABLE IF EXISTS m_tbl_d;
CREATE TEMP TABLE m_tbl_d AS
SELECT tbl_name, LISTAGG(col,', ') 
WITHIN GROUP (ORDER BY id) AS col_d
FROM tbl_cols
GROUP BY tbl_name
ORDER BY tbl_name
;

DROP TABLE IF EXISTS meteoparam_tbl_defs;
CREATE TABLE meteoparam_tbl_defs AS
SELECT tbl_name, CONCAT('datetime DATETIME, ', col_d) col_defs  
FROM m_tbl_d
;

END;
$$ LANGUAGE plpgsql;

