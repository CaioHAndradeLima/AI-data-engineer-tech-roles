terraform {
  required_version = ">= 1.4.0"

  backend "s3" {
    # Terraform state bucket (create this manually once in AWS)
    bucket = "latam-roles-tf-state"
    region = "us-east-1"
    key    = "latam-roles/default.tfstate" # overridden by -backend-config=key=...
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

locals {
  project_name = "latam-roles"
}

resource "aws_s3_bucket" "results" {
  bucket = "${local.project_name}-results-${var.environment}"

  tags = {
    Project     = local.project_name
    Environment = var.environment
  }
}

resource "aws_s3_bucket_public_access_block" "results" {
  bucket = aws_s3_bucket.results.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "results" {
  bucket = aws_s3_bucket.results.id

  versioning_configuration {
    status = "Enabled"
  }
}

#
# Glue Data Catalog: database + external table over S3 JSON results
#

resource "aws_glue_catalog_database" "roles" {
  name = "${local.project_name}_${var.environment}_db"
}

resource "aws_glue_catalog_table" "roles_ai_estimates" {
  name          = "latam_roles_ai_estimates"
  database_name = aws_glue_catalog_database.roles.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification = "json"
    typeOfData     = "file"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.results.bucket}/roles/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    columns {
      name = "total_positions"
      type = "int"
    }

    columns {
      name = "keywords"
      type = "map<string,int>"
    }

    columns {
      name = "note"
      type = "string"
    }
  }
}

resource "aws_iam_openid_connect_provider" "github" {
  url = "https://token.actions.githubusercontent.com"

  client_id_list = [
    "sts.amazonaws.com",
  ]

  # Current GitHub Actions OIDC root certificate thumbprint.
  thumbprint_list = [
    "6938fd4d98bab03faadb97b34396831e3780aea1",
  ]
}

data "aws_iam_policy_document" "github_assume_role" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    # Restrict to this GitHub repo.
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:CaioHAndradeLima/AI-data-engineer-tech-roles:*"]
    }
  }
}

resource "aws_iam_role" "github_actions" {
  name               = "${local.project_name}-gha-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.github_assume_role.json

  tags = {
    Project     = local.project_name
    Environment = var.environment
  }
}

data "aws_iam_policy_document" "github_permissions" {
  statement {
    sid = "S3ResultsAccess"

    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:ListBucket",
    ]

    resources = [
      aws_s3_bucket.results.arn,
      "${aws_s3_bucket.results.arn}/*",
    ]
  }

  statement {
    sid = "S3TerraformStateAccess"

    actions = [
      "s3:ListBucket",
    ]

    resources = [
      "arn:aws:s3:::latam-roles-tf-state",
    ]
  }

  statement {
    sid = "S3TerraformStateObjectAccess"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]

    resources = [
      "arn:aws:s3:::latam-roles-tf-state/*",
    ]
  }
}

resource "aws_iam_policy" "github_permissions" {
  name        = "${local.project_name}-gha-${var.environment}-s3"
  description = "Permissions for GitHub Actions to write results to S3"
  policy      = data.aws_iam_policy_document.github_permissions.json
}

resource "aws_iam_role_policy_attachment" "github_permissions" {
  role       = aws_iam_role.github_actions.name
  policy_arn = aws_iam_policy.github_permissions.arn
}

output "results_bucket_name" {
  value = aws_s3_bucket.results.bucket
}

output "github_actions_role_arn" {
  value = aws_iam_role.github_actions.arn
}

output "glue_database_name" {
  value = aws_glue_catalog_database.roles.name
}

output "glue_table_name" {
  value = aws_glue_catalog_table.roles_ai_estimates.name
}
