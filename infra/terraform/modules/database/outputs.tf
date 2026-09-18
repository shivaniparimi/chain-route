output "endpoint" {
  value = aws_db_instance.main.endpoint
}

output "secret_arn" {
  value = aws_secretsmanager_secret.db.arn
}

output "connection_url_secret_arn" {
  value = aws_secretsmanager_secret.connection_url.arn
}
