variable "environment" {
  description = "Deployment environment name (e.g. dev, prod), used in resource name prefixes."
  type        = string
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC."
  type        = string
  default     = "10.20.0.0/16"
}
