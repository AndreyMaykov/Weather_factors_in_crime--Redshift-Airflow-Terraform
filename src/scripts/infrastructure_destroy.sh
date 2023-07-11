# Object: Shell script
# Terraform version: 1.3.7 on windows_386

# Author: Andrey Maykov
# Date: 2023-03-31

# Description:
# The script destroys the project's infrastructure 
# created using infrastructure_deploy.sh.


#!/bin/bash

source .env

terraform init
terraform destroy \
	-var-file="dev.tfvars" \
	-var="aws_region=$aws_region" \
	-var="rs_cluster_identifier=$cluster_identifier" \
	-var="rs_database_name=$database_name" \
	-var="redshift_role_wxcr=$redshiftIAMRole" \
	-var="rs_master_username=$login" \
	-var="password=$password" \
	-var="s3_bucket=$s3_bucket" \
	-auto-approve