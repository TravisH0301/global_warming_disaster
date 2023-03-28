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
resource "aws_s3_object" "ingress_loader" {
  bucket = aws_s3_bucket.scripts.id
  key    = "ingress_loader.zip"
  source = "../code/ingress_loader.zip"
}

resource "aws_s3_object" "ingress_to_bronze" {
  bucket = aws_s3_bucket.scripts.id
  key    = "ingress_to_bronze.py"
  source = "../code/ingress_to_bronze.py"
}

resource "aws_s3_object" "bronze_to_silver" {
  bucket = aws_s3_bucket.scripts.id
  key    = "bronze_to_silver.py"
  source = "../code/bronze_to_silver.py"
}

resource "aws_s3_object" "silver_to_gold" {
  bucket = aws_s3_bucket.scripts.id
  key    = "silver_to_gold.py"
  source = "../code/silver_to_gold.py"
}