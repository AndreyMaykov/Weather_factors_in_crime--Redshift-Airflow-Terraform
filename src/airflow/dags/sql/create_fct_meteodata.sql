/*

Object: SQL statement

AWS Redshift version: 
PostgreSQL 8.0.2 on i686-pc-linux-gnu, compiled by GCC gcc (GCC) 3.4.2 20041017 (Red Hat 3.4.2-6.fc3), Redshift 1.0.48805

Author: Andrey Maykov

Date: 2023-03-29

Description:
 
The statement creates the meteorological parameter fact table.
 
*/


CREATE TABLE wxcr.fct_meteodata AS
SELECT dd.id date_id, dh.id hour_id, md.* FROM
wxcr_be.meteodata_unpivoted md
JOIN wxcr.dim_dates dd
ON DATE(md.datetime) = dd.date
JOIN wxcr.dim_hours dh
ON EXTRACT(hour FROM md.datetime) = dh.id
;
