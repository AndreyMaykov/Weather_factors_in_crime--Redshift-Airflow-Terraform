/*

Object: stored procedure 

AWS Redshift version: 
PostgreSQL 8.0.2 on i686-pc-linux-gnu, compiled by GCC gcc (GCC) 3.4.2 20041017 (Red Hat 3.4.2-6.fc3), Redshift 1.0.48805

Author: Andrey Maykov

Date: 2023-03-29

Description:
 
See the comment on the create_dim_dates task
in weather_factors_in_crime.py
 
*/


CREATE OR REPLACE PROCEDURE wxcr_be.create_dim_dates(start_not_later IN DATE, end_not_earlier IN DATE)
AS $$


DECLARE dr_beginning_meteo DATE;
DECLARE dr_end_meteo DATE;

DECLARE dr_beginning_ci DATE;
DECLARE dr_end_ci DATE;

DECLARE dr_beginning VARCHAR(12);
DECLARE dr_end VARCHAR(12);


DECLARE sql VARCHAR(MAX);


BEGIN

-- Determine the earliest and the latest incident date in the cleaned crime incident dataset
SELECT INTO dr_beginning_ci MIN(incident_date) FROM wxcr_be.ci2;
SELECT INTO dr_end_ci MAX(incident_date) FROM wxcr_be.ci2;

-- Determine the earliest and the latest measurment date in the cleaned meteo dataset
SELECT INTO dr_beginning_meteo MIN(DATE(datetime)) FROM wxcr_be.meteodata_unpivoted;
SELECT INTO dr_end_meteo MAX(DATE(datetime)) FROM wxcr_be.meteodata_unpivoted;

CALL wxcr_be.create_date_range(
	  LEAST(dr_beginning_meteo, dr_beginning_ci, start_not_later)::VARCHAR(12)
	, GREATEST(dr_end_meteo, dr_end_ci, end_not_earlier)::VARCHAR(12)
);

END;
$$ LANGUAGE plpgsql;


