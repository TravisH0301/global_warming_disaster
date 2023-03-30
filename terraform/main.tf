# Configure Terraform
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.16"
    }
  }

  required_version = ">= 1.2.0"
}

# Configure AWS provider
provider "aws" {
  region  = var.aws_region
}

# Configure S3 bucket
## Create S3 buckets
resource "aws_s3_bucket" "scripts" {
  bucket = "gwd-scripts"
  force_destroy = true  # delete everything stored when destroyed
}

resource "aws_s3_bucket" "airflow-dags" {
  bucket = "gwd-dags"
  force_destroy = true  # delete everything stored when destroyed
}

resource "aws_s3_bucket" "ingress" {
  bucket = "gwd-ingress"
  force_destroy = true  # delete everything stored when destroyed
}

resource "aws_s3_bucket" "bronze" {
  bucket = "gwd-bronze"
  force_destroy = true  # delete everything stored when destroyed
}

resource "aws_s3_bucket" "silver" {
  bucket = "gwd-silver"
  force_destroy = true  # delete everything stored when destroyed
}

resource "aws_s3_bucket" "gold" {
  bucket = "gwd-gold"
  force_destroy = true  # delete everything stored when destroyed
}

# Upload Python scripts to S3 bucket
resource "aws_s3_object" "ingress-to-bronze" {
  bucket = aws_s3_bucket.scripts.id
  key    = "ingress_to_bronze.py"
  source = "../code/ingress_to_bronze.py"
}

resource "aws_s3_object" "bronze-to-silver" {
  bucket = aws_s3_bucket.scripts.id
  key    = "bronze_to_silver.py"
  source = "../code/bronze_to_silver.py"
}

resource "aws_s3_object" "silver-to-gold" {
  bucket = aws_s3_bucket.scripts.id
  key    = "silver_to_gold.py"
  source = "../code/silver_to_gold.py"
}

resource "aws_s3_object" "airflow-dag" {
  bucket = aws_s3_bucket.airflow-dags.id
  key    = "dags/airflow_dag.py"
  source = "../code/airflow_dag.py"
}

# Upload dependencies to S3 bucket
resource "aws_s3_object" "lambda-dep" {
  bucket = aws_s3_bucket.scripts.id
  key    = "python.zip"
  source = "../dependency/python.zip"
}

resource "aws_s3_object" "glue-delta-dep" {
  bucket = aws_s3_bucket.scripts.id
  key    = "delta-core_2.12-1.0.0.jar"
  source = "../dependency/delta-core_2.12-1.0.0.jar"
}

resource "aws_s3_object" "airflow-dag-dep" {
  bucket = aws_s3_bucket.airflow-dags.id
  key    = "dags/requirement.txt"
  source = "../dependency/airflow_dag_requirement.txt"
}