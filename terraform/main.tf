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
