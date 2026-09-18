# Compute/networking shell for the observability stack (OTel Collector,
# Jaeger, Prometheus, Grafana) -- four independent Fargate services, each
# via the reusable `ecs-service` module (Task 7). This module has no
# knowledge of go-server/go-worker/cpp-router: it never references an app
# service, and nothing here (or in `ecs-service`, or in the root wiring)
# ever makes an app service's `depends_on` reference one of these. This
# mirrors the Docker Compose constraint that observability never gates
# app startup -- here it's simply a structural fact of this being a
# separate module with no cross-references.
#
# Toggling: this module always declares its four services. Whether it is
# instantiated at all is decided at the ROOT level (design doc §4.3) via
# a root-level `count`/conditional module call gated on
# `enable_observability_stack` -- not inside this module.
#
# KNOWN LIMITATION (disclosed, not fixed here): unlike the local Docker
# Compose setup, these four Fargate services have no direct equivalent of
# Compose's config-file bind-mounts. Provisioning the actual Prometheus
# scrape-config or Grafana dashboard/datasource content into these cloud
# containers is NOT done in this module -- the upstream `prom/prometheus`
# and `grafana/grafana` images are used as-is, unconfigured beyond their
# defaults. Closing this gap would require either:
#   - a small custom image per service with the config baked in via a
#     Dockerfile `COPY` layered on top of the upstream image, or
#   - an EFS-backed volume mounted into the task, populated out-of-band.
# Building either of those for four optional, toggleable observability
# containers in a portfolio-scale deployment is disproportionate
# complexity (design doc §4.3); this module intentionally provisions only
# the compute/networking shell and leaves config-delivery as a named
# follow-up.

module "otel_collector" {
  source = "../ecs-service"

  environment                    = var.environment
  service_name                   = "otel-collector"
  cluster_id                     = var.cluster_id
  image                          = "otel/opentelemetry-collector-contrib:0.113.0"
  container_port                 = 4317
  cpu                            = 256
  memory                         = 512
  subnet_ids                     = var.private_subnet_ids
  security_group_ids             = [var.internal_security_group_id]
  log_group_name                 = var.log_group_name
  execution_role_arn             = var.execution_role_arn
  task_role_arn                  = var.task_role_arn
  service_discovery_namespace_id = var.service_discovery_namespace_id
}

module "jaeger" {
  source = "../ecs-service"

  environment    = var.environment
  service_name   = "jaeger"
  cluster_id     = var.cluster_id
  image          = "jaegertracing/all-in-one:1.63.0"
  container_port = 16686
  cpu            = 256
  memory         = 512
  environment_variables = {
    COLLECTOR_OTLP_ENABLED = "true"
  }
  subnet_ids                     = var.private_subnet_ids
  security_group_ids             = [var.internal_security_group_id]
  log_group_name                 = var.log_group_name
  execution_role_arn             = var.execution_role_arn
  task_role_arn                  = var.task_role_arn
  service_discovery_namespace_id = var.service_discovery_namespace_id
}

module "prometheus" {
  source = "../ecs-service"

  environment                    = var.environment
  service_name                   = "prometheus"
  cluster_id                     = var.cluster_id
  image                          = "prom/prometheus:v2.55.1"
  container_port                 = 9090
  cpu                            = 256
  memory                         = 512
  subnet_ids                     = var.private_subnet_ids
  security_group_ids             = [var.internal_security_group_id]
  log_group_name                 = var.log_group_name
  execution_role_arn             = var.execution_role_arn
  task_role_arn                  = var.task_role_arn
  service_discovery_namespace_id = var.service_discovery_namespace_id
}

module "grafana" {
  source = "../ecs-service"

  environment    = var.environment
  service_name   = "grafana"
  cluster_id     = var.cluster_id
  image          = "grafana/grafana:11.3.1"
  container_port = 3000
  cpu            = 256
  memory         = 512
  environment_variables = {
    GF_AUTH_ANONYMOUS_ENABLED  = "true"
    GF_AUTH_ANONYMOUS_ORG_ROLE = "Admin"
  }
  subnet_ids                     = var.private_subnet_ids
  security_group_ids             = [var.internal_security_group_id]
  log_group_name                 = var.log_group_name
  execution_role_arn             = var.execution_role_arn
  task_role_arn                  = var.task_role_arn
  service_discovery_namespace_id = var.service_discovery_namespace_id
}
