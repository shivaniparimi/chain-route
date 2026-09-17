# Intentionally no outputs: nothing in the root wiring reads from this
# module. Each underlying `ecs-service` instance still surfaces its own
# `service_name` / `task_definition_arn` outputs (module.otel_collector.*,
# module.jaeger.*, module.prometheus.*, module.grafana.*) if ever needed.
