variable "environment" {
  type = string
}

variable "service_name" {
  description = "Short name, e.g. \"go-server\", used in resource names and the Cloud Map service name."
  type        = string
}

variable "cluster_id" {
  type = string
}

variable "image" {
  description = "Full ECR image URI including tag."
  type        = string
}

variable "container_port" {
  type = number
}

variable "cpu" {
  type    = number
  default = 256
}

variable "memory" {
  type    = number
  default = 512
}

variable "desired_count" {
  type    = number
  default = 1
}

variable "subnet_ids" {
  type = list(string)
}

variable "security_group_ids" {
  type = list(string)
}

variable "environment_variables" {
  description = "Plain (non-secret) container env vars."
  type        = map(string)
  default     = {}
}

variable "secrets" {
  description = "Map of container env var name -> Secrets Manager ARN (with an optional ::jsonkey suffix), injected via the ECS task definition's `secrets` block, never a literal value."
  type        = map(string)
  default     = {}
}

variable "log_group_name" {
  type = string
}

variable "execution_role_arn" {
  type = string
}

variable "task_role_arn" {
  type = string
}

variable "service_discovery_namespace_id" {
  description = "If set, registers this service in Cloud Map under this namespace for internal DNS resolution (e.g. cpp-router.chainroute.local)."
  type        = string
  default     = null
}

variable "target_group_arn" {
  description = "If set, attaches this service to an ALB target group (only go-server should ever set this)."
  type        = string
  default     = null
}
