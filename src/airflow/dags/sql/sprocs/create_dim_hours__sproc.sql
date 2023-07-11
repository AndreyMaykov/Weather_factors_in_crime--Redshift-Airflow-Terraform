/*

Object: stored procedure 

AWS Redshift version: 
PostgreSQL 8.0.2 on i686-pc-linux-gnu, compiled by GCC gcc (GCC) 3.4.2 20041017 (Red Hat 3.4.2-6.fc3), Redshift 1.0.48805

Author: Andrey Maykov

Date: 2023-03-29

Description:
 
The procedure creates the table containing  
the consequtive hour values 00:00, 01:00, ... , 23:00
 
*/


CREATE OR REPLACE PROCEDURE wxcr_be.create_dim_hours()
AS $$

BEGIN

DROP TABLE IF EXISTS hour_ids;
CREATE TEMP TABLE hour_ids AS
WITH RECURSIVE ids(id) AS (
	SELECT(0) 
	UNION ALL
	SELECT(id + 1) FROM ids WHERE id < 23
)
SELECT * FROM ids;


DROP TABLE IF EXISTS pre_dim_hours;
CREATE TEMP TABLE pre_dim_hours AS 
SELECT
	  id
	, dateadd(hour, id, '2030-01-01')::TIME AS hour
FROM hour_ids
; 

DROP TABLE IF EXISTS wxcr.dim_hours;
CREATE TABLE wxcr.dim_hours (
	  id INT PRIMARY KEY
	, hour TIME
)
DISTSTYLE ALL
SORTKEY(hour)
;

INSERT INTO wxcr.dim_hours
SELECT * FROM pre_dim_hours
;



END;
$$ LANGUAGE plpgsql;