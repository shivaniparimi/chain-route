# Single-node Redpanda on ECS Fargate -- deliberately NOT MSK/MSK
# Serverless (design doc §4.1): mirrors the local Docker Compose setup
# exactly, costs only one small Fargate task, and is explicitly NOT
# production-grade (no replication, no HA -- a task restart loses
# in-flight uncommitted data the same way a local dev restart would).
# The production upgrade path is MSK Serverless or a multi-broker
# Redpanda cluster across AZs; deliberately not built here, since doing
# so would be exactly the "unnecessarily expensive production-scale
# Kafka cluster to look sophisticated" this phase was told not to add.
module "redpanda_service" {
  source = "../ecs-service"

  environment                    = var.environment
  service_name                   = "redpanda"
  cluster_id                     = var.cluster_id
  image                          = "docker.redpanda.com/redpandadata/redpanda:v24.2.7"
  container_port                 = 9092
  cpu                            = 512
  memory                         = 1024
  subnet_ids                     = var.private_subnet_ids
  security_group_ids             = [var.internal_security_group_id]
  log_group_name                 = var.log_group_name
  execution_role_arn             = var.execution_role_arn
  task_role_arn                  = var.task_role_arn
  service_discovery_namespace_id = var.service_discovery_namespace_id
  # Same tuning flags as the local Docker Compose redpanda service, except
  # --advertise-kafka-addr uses Redpanda's own Cloud Map DNS name instead
  # of Compose's service name, matching outputs.tf's bootstrap_endpoint.
  command = [
    "redpanda", "start",
    "--smp=1",
    "--memory=512M",
    "--overprovisioned",
    "--node-id=0",
    "--check=false",
    "--kafka-addr=PLAINTEXT://0.0.0.0:9092",
    "--advertise-kafka-addr=PLAINTEXT://redpanda.chainroute.local:9092",
  ]
}
