# Create AWS region variable
variable "aws_region" {
  description = "Region for AWS"
  type        = string
  default     = "ap-southeast-2"
}

# Create S3 bucket name variable
variable "s3_bucket_name" {
  description = "Name for S3 bucket"
  type        = string
  default     = "s3_bucket"
}