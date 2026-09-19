variable "environment" {
  type = string
}

variable "acm_certificate_arn" {
  description = "Optional ACM certificate ARN for a custom domain -- MUST be in us-east-1 regardless of the deployment's own aws_region, since that is a hard CloudFront requirement, not a ChainRoute choice. Null uses CloudFront's own default *.cloudfront.net certificate (works out of the box, no custom domain)."
  type        = string
  default     = null
}
