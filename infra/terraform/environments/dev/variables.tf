variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "db_instance_class" {
  type    = string
  default = "db.t4g.micro"
}

variable "enable_observability_stack" {
  description = "Toggle the Prometheus/OTel-Collector/Jaeger/Grafana ECS services entirely -- the explicit cost/complexity escape hatch (design doc §4.3)."
  type        = bool
  default     = true
}

variable "enable_frontend" {
  description = "Toggle the S3+CloudFront static hosting for the payment analytics dashboard entirely -- the same cost/complexity escape hatch pattern as enable_observability_stack."
  type        = bool
  default     = true
}

variable "acm_certificate_arn" {
  description = "ACM certificate ARN for the ALB's HTTPS listener -- placeholder in terraform.tfvars.example, must be supplied per-deployment for a real domain."
  type        = string
}

variable "image_tag" {
  description = "Image tag to deploy for all three app services (typically a git SHA from CI)."
  type        = string
  default     = "latest"
}
