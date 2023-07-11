/*

Object: stored procedure 

AWS Redshift version: 
PostgreSQL 8.0.2 on i686-pc-linux-gnu, compiled by GCC gcc (GCC) 3.4.2 20041017 (Red Hat 3.4.2-6.fc3), Redshift 1.0.48805

Author: Andrey Maykov

Date: 2023-03-26

Description:
 
The procedure 
1. determins the maximum number of the "standard" offense names 
that comprise a single "compound" offense name 
(i.e. the semicolon-separated string that is used in the
the original file crime_incidents.csv to describe
an individual crime indcident);
2. splits each "compound" offense name into the "standard"
offense names;
3. creates the table ci_offense_names_normalized. In this table,
every incident is represented by rows each of which
contains one "standard" offense name from the incident's
original "compound" offense name (column offense_name), 
along with the number of the current "standard" offense name 
(column num) and the total number of the incident's "standard" 
offense names (column name_cnt).

*/
	
			

CREATE OR REPLACE PROCEDURE wxcr_be.split_offenses_ci()
AS $$

DECLARE sql VARCHAR(MAX);
DECLARE max_offenses_per_incident INT;

BEGIN

SET search_path = wxcr_be;
	
CREATE TEMP TABLE ci_offense_name_counts AS
		SELECT id, offenses, regexp_count(offenses, ';') + 1 AS name_cnt
		FROM ci2;

SELECT INTO max_offenses_per_incident 
MAX(name_cnt) 
FROM ci_offense_name_counts;

CREATE TEMP TABLE numbers AS
WITH RECURSIVE nums(num) AS 
(
SELECT 1  
UNION ALL
SELECT num + 1 FROM nums WHERE num < max_offenses_per_incident
)
SELECT * FROM nums;

CREATE TABLE ci_offense_names_normalized AS
SELECT 
	  onc.id
	, onc.name_cnt
	, n.num
	, split_part(onc.offenses,';', num) offense_name
FROM numbers n, ci_offense_name_counts onc
WHERE n.num <= onc.name_cnt
;

END;
$$ LANGUAGE plpgsql;


