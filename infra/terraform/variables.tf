variable "environment" {
  description = "Deployment environment (e.g. dev, prod)"
  type        = string
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

