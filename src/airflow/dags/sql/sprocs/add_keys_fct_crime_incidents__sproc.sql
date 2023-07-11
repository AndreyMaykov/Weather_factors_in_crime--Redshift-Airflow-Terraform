/*

Object: SQL stored procedure

AWS Redshift version: 
PostgreSQL 8.0.2 on i686-pc-linux-gnu, compiled by GCC gcc (GCC) 3.4.2 20041017 (Red Hat 3.4.2-6.fc3), Redshift 1.0.48805

Author: Andrey Maykov

Date: 2023-03-31

Description:
 
The procedure adds a distribution key, a compound sort key, 
and foreign keys to the fact table fct_crime_incidents.

*/

CREATE OR REPLACE PROCEDURE wxcr_be.add_keys_fct_crime_incidents()
AS $$

BEGIN

ALTER TABLE wxcr.fct_crime_incidents 
  ALTER COMPOUND SORTKEY (ori, date_id, hour_id)
, ALTER DISTKEY ori
;

ALTER TABLE wxcr.fct_crime_incidents 
ADD CONSTRAINT fk_ori FOREIGN KEY (ori)
REFERENCES wxcr.dim_agencies (ori)
;

ALTER TABLE wxcr.fct_crime_incidents 
ADD CONSTRAINT fk_date_id FOREIGN KEY (date_id)
REFERENCES wxcr.dim_dates (id)
;


ALTER TABLE wxcr.fct_crime_incidents 
ADD CONSTRAINT fk_hour_id FOREIGN KEY (hour_id)
REFERENCES wxcr.dim_hours (id)
;


END;
$$ LANGUAGE plpgsql;