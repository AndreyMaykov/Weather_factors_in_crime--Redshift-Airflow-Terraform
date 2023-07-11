/*

Object: Terraform file

Terraform version: 1.3.7 on windows_386

Author: Andrey Maykov

Date: 2023-03-31

Description:

contains the environment variables' values
required for deploying the project's infrastructure
using full.tf and infrastructure_deploy.sh

*/

rs_nodetype = "dc2.large"

rs_cluster_type = "single-node"

vpc_cidr = "10.0.0.0/16"

redshift_subnet_cidr_1 = "10.0.1.0/24"

redshift_subnet_cidr_2 = "10.0.2.0/24"
