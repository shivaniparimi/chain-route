resource "random_password" "db" {
  length  = 32
  special = false
}

resource "aws_secretsmanager_secret" "db" {
  name_prefix = "${var.environment}-chainroute-db-"
}

resource "aws_secretsmanager_secret_version" "db" {
  secret_id = aws_secretsmanager_secret.db.id
  secret_string = jsonencode({
    username = "chainroute"
    password = random_password.db.result
  })
}

resource "aws_db_subnet_group" "main" {
  name_prefix = "${var.environment}-chainroute-"
  subnet_ids  = var.private_subnet_ids
}

resource "aws_db_instance" "main" {
  identifier_prefix       = "${var.environment}-chainroute-"
  engine                  = "postgres"
  engine_version          = "16"
  instance_class          = var.instance_class
  allocated_storage       = 20
  db_name                 = "chainroute"
  username                = "chainroute"
  password                = random_password.db.result
  db_subnet_group_name    = aws_db_subnet_group.main.name
  vpc_security_group_ids  = [var.data_security_group_id]
  publicly_accessible     = false
  backup_retention_period = 7
  skip_final_snapshot     = var.environment != "prod"
  tags                    = { Name = "${var.environment}-chainroute-postgres" }
}

# Full connection string (including the password), stored as its own
# secret so the Go application -- which only reads a single DATABASE_URL
# env var, not a separate password var -- can consume it directly via the
# ECS task definition's `secrets` block.
resource "aws_secretsmanager_secret" "connection_url" {
  name_prefix = "${var.environment}-chainroute-db-url-"
}

resource "aws_secretsmanager_secret_version" "connection_url" {
  secret_id     = aws_secretsmanager_secret.connection_url.id
  secret_string = "postgres://chainroute:${random_password.db.result}@${aws_db_instance.main.endpoint}/chainroute?sslmode=require"
}
