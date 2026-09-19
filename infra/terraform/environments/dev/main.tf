provider "aws" {
  region = var.aws_region
}

module "networking" {
  source      = "../../modules/networking"
  environment = var.environment
}

resource "aws_cloudwatch_log_group" "chainroute" {
  name              = "/ecs/${var.environment}-chainroute"
  retention_in_days = 14
}

resource "aws_ecs_cluster" "main" {
  name = "${var.environment}-chainroute"
}

resource "aws_iam_role" "ecs_execution" {
  name_prefix = "${var.environment}-chainroute-exec-"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_execution" {
  role       = aws_iam_role.ecs_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "ecs_task" {
  name_prefix        = "${var.environment}-chainroute-task-"
  assume_role_policy = aws_iam_role.ecs_execution.assume_role_policy
}

module "database" {
  source                 = "../../modules/database"
  environment            = var.environment
  private_subnet_ids     = module.networking.private_subnet_ids
  data_security_group_id = module.networking.data_security_group_id
  instance_class         = var.db_instance_class
}

# The task definition's `secrets` block requires the execution role to be
# able to read those specific secrets (separate from
# AmazonECSTaskExecutionRolePolicy, which only covers ECR pull + logs) --
# without this, ECS cannot launch any task that references a secret.
resource "aws_iam_role_policy" "ecs_execution_secrets" {
  name_prefix = "${var.environment}-chainroute-secrets-"
  role        = aws_iam_role.ecs_execution.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "secretsmanager:GetSecretValue"
      Resource = [module.database.secret_arn, module.database.connection_url_secret_arn]
    }]
  })
}

module "messaging" {
  source                         = "../../modules/messaging"
  environment                    = var.environment
  cluster_id                     = aws_ecs_cluster.main.id
  private_subnet_ids             = module.networking.private_subnet_ids
  internal_security_group_id     = module.networking.internal_security_group_id
  log_group_name                 = aws_cloudwatch_log_group.chainroute.name
  execution_role_arn             = aws_iam_role.ecs_execution.arn
  task_role_arn                  = aws_iam_role.ecs_task.arn
  service_discovery_namespace_id = module.networking.service_discovery_namespace_id
}

# The Prometheus/OTel-Collector/Jaeger/Grafana stack is an explicit
# cost/complexity escape hatch (design doc §4.3): toggled entirely off via
# `enable_observability_stack` for a cheaper dev loop, without touching the
# module itself.
module "observability" {
  count  = var.enable_observability_stack ? 1 : 0
  source = "../../modules/observability"

  environment                    = var.environment
  cluster_id                     = aws_ecs_cluster.main.id
  private_subnet_ids             = module.networking.private_subnet_ids
  internal_security_group_id     = module.networking.internal_security_group_id
  log_group_name                 = aws_cloudwatch_log_group.chainroute.name
  execution_role_arn             = aws_iam_role.ecs_execution.arn
  task_role_arn                  = aws_iam_role.ecs_task.arn
  service_discovery_namespace_id = module.networking.service_discovery_namespace_id
}

module "alb" {
  source                = "../../modules/alb"
  environment           = var.environment
  vpc_id                = module.networking.vpc_id
  public_subnet_ids     = module.networking.public_subnet_ids
  alb_security_group_id = module.networking.alb_security_group_id
  acm_certificate_arn   = var.acm_certificate_arn
}

# S3 + CloudFront static hosting for the read-only payment analytics
# dashboard -- no Fargate service needed since it's static assets, kept
# behind a toggle for the same cost/complexity escape hatch pattern as
# `enable_observability_stack`.
module "frontend" {
  count  = var.enable_frontend ? 1 : 0
  source = "../../modules/frontend"

  environment = var.environment
}

resource "aws_ecr_repository" "go_server" {
  name = "${var.environment}-chainroute-go-server"
}

resource "aws_ecr_repository" "go_worker" {
  name = "${var.environment}-chainroute-go-worker"
}

resource "aws_ecr_repository" "cpp_router" {
  name = "${var.environment}-chainroute-cpp-router"
}

resource "aws_ecr_lifecycle_policy" "bounded_retention" {
  for_each   = { server = aws_ecr_repository.go_server.name, worker = aws_ecr_repository.go_worker.name, router = aws_ecr_repository.cpp_router.name }
  repository = each.value
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep only the last 10 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 10
      }
      action = { type = "expire" }
    }]
  })
}

module "go_server_service" {
  source = "../../modules/ecs-service"

  environment        = var.environment
  service_name       = "go-server"
  cluster_id         = aws_ecs_cluster.main.id
  image              = "${aws_ecr_repository.go_server.repository_url}:${var.image_tag}"
  container_port     = 8080
  subnet_ids         = module.networking.private_subnet_ids
  security_group_ids = [module.networking.app_security_group_id]
  environment_variables = {
    OTEL_EXPORTER_OTLP_ENDPOINT     = "otel-collector.chainroute.local:4317"
    CHAINROUTE_CORS_ALLOWED_ORIGINS = var.enable_frontend ? "https://${module.frontend[0].cloudfront_domain_name}" : "http://localhost:5173"
  }
  secrets            = { DATABASE_URL = module.database.connection_url_secret_arn }
  command            = ["--http-addr=:8080", "--grpc-addr=cpp-router.chainroute.local:50051"]
  log_group_name     = aws_cloudwatch_log_group.chainroute.name
  execution_role_arn = aws_iam_role.ecs_execution.arn
  task_role_arn      = aws_iam_role.ecs_task.arn
  target_group_arn   = module.alb.target_group_arn
}

module "go_worker_service" {
  source = "../../modules/ecs-service"

  environment        = var.environment
  service_name       = "go-worker"
  cluster_id         = aws_ecs_cluster.main.id
  image              = "${aws_ecr_repository.go_worker.repository_url}:${var.image_tag}"
  container_port     = 9091
  subnet_ids         = module.networking.private_subnet_ids
  security_group_ids = [module.networking.internal_security_group_id]
  environment_variables = {
    KAFKA_BOOTSTRAP_SERVERS     = module.messaging.bootstrap_endpoint
    OTEL_EXPORTER_OTLP_ENDPOINT = "otel-collector.chainroute.local:4317"
  }
  secrets            = { DATABASE_URL = module.database.connection_url_secret_arn }
  log_group_name     = aws_cloudwatch_log_group.chainroute.name
  execution_role_arn = aws_iam_role.ecs_execution.arn
  task_role_arn      = aws_iam_role.ecs_task.arn
}

module "cpp_router_service" {
  source = "../../modules/ecs-service"

  environment                    = var.environment
  service_name                   = "cpp-router"
  cluster_id                     = aws_ecs_cluster.main.id
  image                          = "${aws_ecr_repository.cpp_router.repository_url}:${var.image_tag}"
  container_port                 = 50051
  subnet_ids                     = module.networking.private_subnet_ids
  security_group_ids             = [module.networking.internal_security_group_id]
  log_group_name                 = aws_cloudwatch_log_group.chainroute.name
  execution_role_arn             = aws_iam_role.ecs_execution.arn
  task_role_arn                  = aws_iam_role.ecs_task.arn
  service_discovery_namespace_id = module.networking.service_discovery_namespace_id
}
