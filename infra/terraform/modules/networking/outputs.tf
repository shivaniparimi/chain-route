output "vpc_id" {
  value = aws_vpc.main.id
}

output "public_subnet_ids" {
  value = aws_subnet.public[*].id
}

output "private_subnet_ids" {
  value = aws_subnet.private[*].id
}

output "alb_security_group_id" {
  value = aws_security_group.alb.id
}

output "app_security_group_id" {
  value = aws_security_group.app.id
}

output "internal_security_group_id" {
  value = aws_security_group.internal.id
}

output "data_security_group_id" {
  value = aws_security_group.data.id
}

output "service_discovery_namespace_id" {
  value = aws_service_discovery_private_dns_namespace.internal.id
}
