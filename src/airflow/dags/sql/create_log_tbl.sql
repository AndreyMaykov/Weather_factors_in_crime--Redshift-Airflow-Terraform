/*

Object: SQL statement

AWS Redshift version: 
PostgreSQL 8.0.2 on i686-pc-linux-gnu, compiled by GCC gcc (GCC) 3.4.2 20041017 (Red Hat 3.4.2-6.fc3), Redshift 1.0.48805

Author: Andrey Maykov

Date: 2023-03-25

Description:
 
The statement creates a backend table 
for holding records that document performed
data cleansing operations.
 
*/


CREATE TABLE wxcr_be.log_table (
                  schema_name VARCHAR(64)
                , table_name VARCHAR(64)
                , message VARCHAR(MAX)
                , dt TIMESTAMP
);