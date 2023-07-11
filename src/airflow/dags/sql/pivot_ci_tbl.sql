/*

Object: SQL statement

AWS Redshift version: 
PostgreSQL 8.0.2 on i686-pc-linux-gnu, compiled by GCC gcc (GCC) 3.4.2 20041017 (Red Hat 3.4.2-6.fc3), Redshift 1.0.48805

Author: Andrey Maykov

Date: 2023-03-31

Description:
 
The statement is used with the RedshiftSQLOperator to reorganize the crime incidents 
data so that for each "standard" offense name, a separate column holds boolean values
indicating whether the name is pertinent to a given incident. 
 
*/


CREATE TABLE wxcr_be.ci_offense_names_denormalized AS
    SELECT * FROM wxcr_be.ci_offense_names_normalized
    PIVOT (
        COUNT(num) FOR offense_name IN (
            {{ ti.xcom_pull(key="return_value") }}
        )
    );