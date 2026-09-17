variable "environment" { type = string }
variable "cluster_id" { type = string }
variable "private_subnet_ids" { type = list(string) }
variable "internal_security_group_id" { type = string }
variable "log_group_name" { type = string }
variable "execution_role_arn" { type = string }
variable "task_role_arn" { type = string }
variable "service_discovery_namespace_id" { type = string }
