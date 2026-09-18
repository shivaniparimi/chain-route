# Remote state (recommended for any team/shared use -- NOT configured by
# default so `terraform init`/`validate` work with only local state in
# this repo, without requiring a pre-existing S3 bucket/DynamoDB table).
#
# To enable: create the bucket + lock table once (e.g. via a separate,
# one-time bootstrap `terraform apply` or the AWS CLI), then uncomment
# and fill in the real names below, and run:
#   terraform init -migrate-state
#
# terraform {
#   backend "s3" {
#     bucket         = "REPLACE-ME-chainroute-tfstate"
#     key            = "dev/terraform.tfstate"
#     region         = "us-east-1"
#     dynamodb_table = "REPLACE-ME-chainroute-tflock"
#     encrypt        = true
#   }
# }
