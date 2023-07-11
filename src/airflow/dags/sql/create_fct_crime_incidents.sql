/*

Object: SQL statement

AWS Redshift version: 
PostgreSQL 8.0.2 on i686-pc-linux-gnu, compiled by GCC gcc (GCC) 3.4.2 20041017 (Red Hat 3.4.2-6.fc3), Redshift 1.0.48805

Author: Andrey Maykov

Date: 2023-03-29

Description:
 
The statement creates the crime incident fact table
 
*/



CREATE TABLE wxcr.fct_crime_incidents AS
	WITH ci3 AS (
		SELECT * FROM
		wxcr_be.ci2
		JOIN wxcr_be.ci_offense_names_denormalized
		USING (id)
	)
SELECT dd.id date_id, dh.id hour_id, ci3.* FROM
ci3 
LEFT JOIN wxcr.dim_dates dd
ON ci3.incident_date = dd.date
LEFT JOIN wxcr.dim_hours dh
ON ci3.incident_time = dh.hour
;
