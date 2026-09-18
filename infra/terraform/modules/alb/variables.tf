variable "environment" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "public_subnet_ids" {
  type = list(string)
}

variable "alb_security_group_id" {
  type = string
}

variable "acm_certificate_arn" {
  description = "ARN of an ACM certificate for the deployer's own domain. No default: the root tfvars.example documents this as a placeholder the deployer must supply."
  type        = string
}
