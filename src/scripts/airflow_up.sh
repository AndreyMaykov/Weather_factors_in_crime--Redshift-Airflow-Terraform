# Object: Shell script
# Requires Docker CE and Docker Compose (v. 1.29.1 or newer) installed locally

# Author: Andrey Maykov
# Date: 2023-03-21

# Description:
# The script installs in a local Docker container
# and configures for the project
# an instance of Apache Airflow 

source .env

source cluster-connection.env

cd ../airflow || exit
docker-compose -f docker-compose.yaml up --detach

docker exec airflow_scheduler airflow variables set s3_bucket "$s3_bucket"

IAM_ROLE_ARN=arn:aws:iam::$account_id:role/$redshiftIAMRole
docker exec airflow_scheduler airflow variables set iam_role_arn $IAM_ROLE_ARN

docker exec airflow_scheduler airflow connections delete postgres_default
docker exec airflow_scheduler airflow connections add 'postgres_default' \
    --conn-json '{
            "conn_type": "postgres",
            "login": "'$login'",
            "password": "'$password'",
            "host": "'$redshift_host'",
            "port": '$redshift_port',
            "schema": "'$database_name'"
        }'

docker exec airflow_scheduler airflow connections delete redshift_default
docker exec airflow_scheduler airflow connections add 'redshift_default' \
    --conn-json '{
            "conn_type": "redshift",
            "login": "'$login'",
            "password": "'$password'",
            "host": "'$redshift_host'",
            "port": '$redshift_port',
            "schema": "'$database_name'",
            "extra": {
                "region": "'$aws_region'"
            }
        }'

docker exec airflow_scheduler airflow connections delete aws_s3_connection
docker exec airflow_scheduler airflow connections add 'aws_s3_connection' \
    --conn-json '{
            "conn_type": "S3",
            "extra": {
                "aws_access_key_id": "'$aws_access_key_id'",
                "aws_secret_access_key": "'$aws_secret_access_key'"
            }
        }'