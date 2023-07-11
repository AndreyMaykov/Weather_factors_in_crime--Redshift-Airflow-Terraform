/*

Object: stored procedure 

AWS Redshift version: 
PostgreSQL 8.0.2 on i686-pc-linux-gnu, compiled by GCC gcc (GCC) 3.4.2 20041017 (Red Hat 3.4.2-6.fc3), Redshift 1.0.48805

Author: Andrey Maykov

Date: 2023-03-24

Description:
 
The procedure joins the the six unpivoted meteoparameter tables
produced by the load_unpivot_meteoparam_tables task group 
on the stn (the station's name) and datetime (the date 
and time of the measurment) columns so that the temperature, 
humidity, etc. datasets are collected in one table.

*/

CREATE OR REPLACE PROCEDURE wxcr_be.join_meteoparam_unpv_tbls()
AS $$

DECLARE str_mid VARCHAR(MAX);
DECLARE str_beginning VARCHAR(MAX);
DECLARE str VARCHAR(MAX);

BEGIN
	
SET search_path = wxcr_be;

DROP TABLE IF EXISTS A;
CREATE TABLE A AS
SELECT 
	  tbl_name
	, ROW_NUMBER() OVER (ORDER BY tbl_name) AS num
FROM meteoparam_names_fmts
;

SELECT LISTAGG(tbl_name, '_unpivoted USING(datetime, stn) FULL JOIN ') 
FROM a 
WHERE num > 1
INTO str_mid
;


SELECT CONCAT('CREATE TABLE meteodata_unpivoted AS SELECT * FROM ',CONCAT(tbl_name, '_unpivoted FULL JOIN '))
FROM a 
WHERE num = 1
INTO str_beginning
;

SELECT CONCAT(CONCAT(str_beginning, str_mid), '_unpivoted USING(datetime, stn);') INTO str
;


EXECUTE str;

END;
$$ LANGUAGE plpgsql;
