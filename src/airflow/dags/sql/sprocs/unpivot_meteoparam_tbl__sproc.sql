/*

Object: stored procedure 

AWS Redshift version: 
PostgreSQL 8.0.2 on i686-pc-linux-gnu, compiled by GCC gcc (GCC) 3.4.2 20041017 (Red Hat 3.4.2-6.fc3), Redshift 1.0.48805

Author: Andrey Maykov

Date: 2023-03-25

Description:

The procedure transforms the meteoparameter tables so that the data 
from all the station-specific columns is placed into a single column 
and the station names corresponding to the data origing is indicated 
in another column).
 
*/

					
					

CREATE OR REPLACE PROCEDURE wxcr_be.unpivot_meteoparam_tbl(
	  tbl_name VARCHAR(64)
)
AS $$

DECLARE col_list VARCHAR(MAX);

BEGIN

SET search_path = wxcr_be;

SELECT LISTAGG(station_name,', ') WITHIN GROUP (ORDER BY id) FROM station_names INTO col_list;

EXECUTE
	'CREATE TABLE '||tbl_name||'_unpivoted AS
	SELECT * FROM '||tbl_name||' UNPIVOT INCLUDE NULLS
	('||tbl_name||' FOR stn in ('||col_list||'))
	ORDER BY datetime;'
;

END;
$$ LANGUAGE plpgsql;
