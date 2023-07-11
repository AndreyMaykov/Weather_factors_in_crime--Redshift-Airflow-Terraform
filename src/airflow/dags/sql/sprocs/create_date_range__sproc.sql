
/*

Object: stored procedure 

AWS Redshift version: 
PostgreSQL 8.0.2 on i686-pc-linux-gnu, compiled by GCC gcc (GCC) 3.4.2 20041017 (Red Hat 3.4.2-6.fc3), Redshift 1.0.48805

Author: Andrey Maykov

Date: 2023-03-29

Description:
 
The procedure creates a table containing the set of consequetive dates
from start_date to end_date in one column and their ids in another.
  
*/


CREATE OR REPLACE PROCEDURE wxcr_be.create_date_range(
	  start_date IN VARCHAR(12)
	, end_date IN VARCHAR(12)
)
AS $$

BEGIN

DROP TABLE IF EXISTS dates_noid;
EXECUTE
	'CREATE TEMP TABLE dates_noid AS
	WITH RECURSIVE dts(dt) AS (
		SELECT(CAST(\''||start_date||'\' AS DATE)) 
		UNION ALL
		SELECT(CAST(DATEADD(DAY, 1, dt) AS DATE)) FROM dts WHERE dt < CAST(\''||end_date||'\' AS DATE)
	)
	SELECT * FROM dts;'
;

DROP TABLE IF EXISTS wxcr.dim_dates;
CREATE TABLE wxcr.dim_dates (
		  id INT IDENTITY(1,1) PRIMARY KEY
		, date DATE
	)
	DISTSTYLE ALL
	SORTKEY(date)
;

INSERT INTO wxcr.dim_dates (date)
	SELECT * FROM dates_noid;

END;
$$ LANGUAGE plpgsql;