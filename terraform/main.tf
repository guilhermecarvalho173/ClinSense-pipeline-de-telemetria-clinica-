terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  s3_use_path_style           = true
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    s3       = "http://127.0.0.1:4566"
    dynamodb = "http://127.0.0.1:4566"
    sns      = "http://127.0.0.1:4566"
    iam      = "http://127.0.0.1:4566"
  }
}

resource "aws_s3_bucket" "raw_lake" {
  bucket        = "clinsense-tf-raw-lake"
  force_destroy = true
}

resource "aws_dynamodb_table" "quente" {
  name         = "clinsense_tf_quente"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "patient_id"
  range_key    = "timestamp"

  attribute {
    name = "patient_id"
    type = "S"
  }

  attribute {
    name = "timestamp"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
}

resource "aws_sns_topic" "alertas" {
  name = "clinsense-tf-alertas"
}