resource "aws_ecs_task_definition" "this" {
  family                   = "${var.environment}-chainroute-${var.service_name}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.cpu
  memory                   = var.memory
  execution_role_arn       = var.execution_role_arn
  task_role_arn            = var.task_role_arn

  container_definitions = jsonencode([merge(
    {
      name      = var.service_name
      image     = var.image
      essential = true
      portMappings = [{
        containerPort = var.container_port
        protocol      = "tcp"
      }]
      environment = [for k, v in var.environment_variables : { name = k, value = v }]
      secrets     = [for k, arn in var.secrets : { name = k, valueFrom = arn }]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = var.log_group_name
          "awslogs-region"        = data.aws_region.current.name
          "awslogs-stream-prefix" = var.service_name
        }
      }
    },
    var.command == null ? {} : { command = var.command }
  )])
}

data "aws_region" "current" {}

resource "aws_service_discovery_service" "this" {
  count = var.service_discovery_namespace_id == null ? 0 : 1
  name  = var.service_name
  dns_config {
    namespace_id = var.service_discovery_namespace_id
    dns_records {
      ttl  = 10
      type = "A"
    }
  }
}

resource "aws_ecs_service" "this" {
  name            = "${var.environment}-chainroute-${var.service_name}"
  cluster         = var.cluster_id
  task_definition = aws_ecs_task_definition.this.arn
  desired_count   = var.desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets         = var.subnet_ids
    security_groups = var.security_group_ids
  }

  dynamic "service_registries" {
    for_each = var.service_discovery_namespace_id == null ? [] : [1]
    content {
      registry_arn = aws_service_discovery_service.this[0].arn
    }
  }

  dynamic "load_balancer" {
    for_each = var.target_group_arn == null ? [] : [1]
    content {
      target_group_arn = var.target_group_arn
      container_name   = var.service_name
      container_port   = var.container_port
    }
  }
}
