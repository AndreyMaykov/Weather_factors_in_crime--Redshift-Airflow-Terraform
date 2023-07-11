/*

Object: SQL statement

AWS Redshift version: 
PostgreSQL 8.0.2 on i686-pc-linux-gnu, compiled by GCC gcc (GCC) 3.4.2 20041017 (Red Hat 3.4.2-6.fc3), Redshift 1.0.48805

Author: Andrey Maykov

Date: 2023-03-25

Description:
 
Used with the RedshiftSQLOperator for loading meteodata
from a project's S3 bucket to a DB table.
The table's and the bucket's name coincide; the common 
name is passed through the parameter tbl_name.

*/


COPY wxcr_be.{{params.tbl_name}} 
FROM 's3://{{params.s3_bucket}}/meteodata/{{params.tbl_name}}.csv' 
IAM_ROLE '{{params.iam_role}}' 
IGNOREHEADER 1 
FORMAT AS CSV 
TIMEFORMAT 'auto';