variable "environment" {
  type = string
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "data_security_group_id" {
  type = string
}

variable "instance_class" {
  type    = string
  default = "db.t4g.micro"
}
