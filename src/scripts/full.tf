/*

Object: Terraform file

Terraform version: 1.3.7 on windows_386

Author: Andrey Maykov

Date: 2023-03-31

Description:
 
The file contains variable declarations, provider configurations, 
resource and output definitions required for deploying 
the project's infrastructure via Terraform

*/





variable "aws_region" { }

variable "rs_cluster_identifier" { }

variable "rs_database_name" { }

variable "redshift_role_wxcr" { }

variable "rs_master_username" { }

variable "password" { }

variable "rs_nodetype" { }

variable "rs_cluster_type" { }

variable "vpc_cidr" { }

variable "redshift_subnet_cidr_1" { }

variable "redshift_subnet_cidr_2" { }

variable "s3_bucket" { }


provider "aws" {
  region = "${var.aws_region}"
}

resource "aws_vpc" "redshift_vpc_wxcr" {
  cidr_block       = "${var.vpc_cidr}"
  instance_tenancy = "default"

  tags = {
    Name = "redshift-vpc-wxcr"
  }
}

resource "aws_internet_gateway" "redshift_vpc_wxcr_gw" {
  vpc_id = "${aws_vpc.redshift_vpc_wxcr.id}"

  depends_on = [
    "aws_vpc.redshift_vpc_wxcr"
  ]
}

resource "aws_subnet" "redshift_subnet_wxcr_1" {
  vpc_id     = "${aws_vpc.redshift_vpc_wxcr.id}"
  cidr_block        = "${var.redshift_subnet_cidr_1}"
  availability_zone = "${var.aws_region}a"
  map_public_ip_on_launch = "true"

  tags = {
    Name = "redshift-subnet-wxcr-1"
  }

  depends_on = [
    "aws_vpc.redshift_vpc_wxcr"
  ]
}

resource "aws_subnet" "redshift_subnet_wxcr_2" {
  vpc_id     = "${aws_vpc.redshift_vpc_wxcr.id}"
  cidr_block        = "${var.redshift_subnet_cidr_2}"
  availability_zone = "${var.aws_region}a"
  map_public_ip_on_launch = "true"

  tags = {
    Name = "redshift-subnet-wxcr-2"
  }

  depends_on = [
    "aws_vpc.redshift_vpc_wxcr"
  ]
}

resource "aws_redshift_subnet_group" "redshift_subnet_wxcr_group" {
  name       = "redshift-subnet-wxcr-group"
  subnet_ids = ["${aws_subnet.redshift_subnet_wxcr_1.id}", "${aws_subnet.redshift_subnet_wxcr_2.id}"]

  tags = {
    environment = "dev"
    Name = "redshift-subnet-wxcr-group"
  }
}

resource "aws_route_table" "redshift_vpc_wxcr_route_table" {
  vpc_id = aws_vpc.redshift_vpc_wxcr.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.redshift_vpc_wxcr_gw.id
  }
}

resource "aws_route_table_association" "redshift_route_table_subnet1_wxcr_association" {
  subnet_id      = aws_subnet.redshift_subnet_wxcr_1.id
  route_table_id = aws_route_table.redshift_vpc_wxcr_route_table.id
}

resource "aws_route_table_association" "redshift_route_table_subnet2_wxcr_association" {
  subnet_id      = aws_subnet.redshift_subnet_wxcr_2.id
  route_table_id = aws_route_table.redshift_vpc_wxcr_route_table.id
}

resource "aws_default_security_group" "redshift_security_group_wxcr" {
  vpc_id     = "${aws_vpc.redshift_vpc_wxcr.id}"

  ingress {
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "redshift-sg-wxcr"
  }

  depends_on = [
    "aws_vpc.redshift_vpc_wxcr"
  ]
}

resource "aws_iam_role_policy" "s3_full_access_policy_wxcr" {
  name = "redshift_s3_policy_wxcr"
  role = "${aws_iam_role.redshift_role_wxcr.id}"

  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "s3:*",
            "Resource": "*"
        }
    ]
}
EOF
}

resource "aws_iam_role" "redshift_role_wxcr" {
  name = "${var.redshift_role_wxcr}"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "redshift.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF

  tags = {
    tag-key = "redshift-role-wxcr"
  }
}

resource "aws_redshift_cluster" "default" {
  cluster_identifier = "${var.rs_cluster_identifier}"
  database_name      = "${var.rs_database_name}"
  master_username    = "${var.rs_master_username}"
  master_password    = "${var.password}"
  node_type          = "${var.rs_nodetype}"
  cluster_type       = "${var.rs_cluster_type}"
  cluster_subnet_group_name = "${aws_redshift_subnet_group.redshift_subnet_wxcr_group.id}"
  skip_final_snapshot = true
  iam_roles = ["${aws_iam_role.redshift_role_wxcr.arn}"]

  depends_on = [
    "aws_vpc.redshift_vpc_wxcr",
    "aws_default_security_group.redshift_security_group_wxcr",
    "aws_redshift_subnet_group.redshift_subnet_wxcr_group",
    "aws_iam_role.redshift_role_wxcr"
  ]
}

resource "aws_s3_bucket" "wxcr_s3_bucket" {
  bucket = "${var.s3_bucket}"

  tags = {
    Name        = "WXCR S3 bucket"
  }
}






output "redshift_host" {
  value = aws_redshift_cluster.default.dns_name
}

output "redshift_port" {
  value = aws_redshift_cluster.default.port
}

resource "local_file" "output_variables" {
  filename = "cluster-connection.env"
  content = <<EOF
redshift_host=${aws_redshift_cluster.default.dns_name}
redshift_port=${aws_redshift_cluster.default.port}
EOF
}